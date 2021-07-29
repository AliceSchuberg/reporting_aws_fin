package com.antra.report.client.service;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.antra.report.client.entity.ExcelReportEntity;
import com.antra.report.client.entity.PDFReportEntity;
import com.antra.report.client.entity.ReportRequestEntity;
import com.antra.report.client.entity.ReportStatus;
import com.antra.report.client.exception.RequestNotFoundException;
import com.antra.report.client.pojo.EmailType;
import com.antra.report.client.pojo.FileType;
import com.antra.report.client.pojo.reponse.*;
import com.antra.report.client.pojo.request.ReportRequest;
import com.antra.report.client.repository.ReportRequestRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Service
public class ReportServiceImpl implements ReportService {
    private static final Logger log = LoggerFactory.getLogger(ReportServiceImpl.class);

    private final ReportRequestRepo reportRequestRepo;
    private final SNSService snsService;
    private final AmazonS3 s3Client;
    private final EmailService emailService;
    private final ThreadPoolService threadPoolService;

    public ReportServiceImpl(ReportRequestRepo reportRequestRepo, SNSService snsService,
                             AmazonS3 s3Client, EmailService emailService,
                             ThreadPoolService threadPoolService) {
        this.reportRequestRepo = reportRequestRepo;
        this.snsService = snsService;
        this.s3Client = s3Client;
        this.emailService = emailService;
        this.threadPoolService = threadPoolService;
    }

    //convert the request info (ReportRequest) into a database Entity, then saved it to repo
    private ReportRequestEntity persistToLocal(ReportRequest request) {
        request.setReqId("Req-"+ UUID.randomUUID().toString());
        /*
        *           ReportRequestEntity  ---> save to Repository
        *           /                \
        *  PDFReportEntity          ExcelReportEntity
        * */

        //create a ReportRequestEntity
        ReportRequestEntity entity = new ReportRequestEntity();
        entity.setReqId(request.getReqId());
        entity.setSubmitter(request.getSubmitter());
        entity.setDescription(request.getDescription());
        entity.setCreatedTime(LocalDateTime.now());

        //create PDFReportEntity and store it into the ReportRequestEntity
        PDFReportEntity pdfReport = new PDFReportEntity();
        pdfReport.setRequest(entity);
        pdfReport.setStatus(ReportStatus.PENDING);
        pdfReport.setCreatedTime(LocalDateTime.now());
        entity.setPdfReport(pdfReport);

        //create ExcelReportEntity, copy the attr from PDF*Entity, store it into Report*Entity
        ExcelReportEntity excelReport = new ExcelReportEntity();
        BeanUtils.copyProperties(pdfReport, excelReport);
        entity.setExcelReport(excelReport);

        //save the RequestEntity into database
        return reportRequestRepo.save(entity);
    }

    @Override
    public ReportVO generateReportsSync(ReportRequest request) {
        //save the ReportRequest object into local repo
        persistToLocal(request);
        //send out request in sync (it will wait for the response, then store into repo)
        sendDirectRequests(request);
        //retrieve the response directly from repo and return
        return new ReportVO(reportRequestRepo.findById(request.getReqId()).orElseThrow());
    }

    //TODO: Change to parallel process using Threadpool / CompletableFuture
    private void sendDirectRequests(ReportRequest request) {
        RestTemplate rs = new RestTemplate();

        //Using CompletableFuture to run the excel request Async
        CompletableFuture<Void> excelFuture = CompletableFuture.runAsync(()->{
            ExcelResponse excelResponse = new ExcelResponse();
            try {
                excelResponse =
                        rs.postForEntity("http://localhost:8888/excel", request, ExcelResponse.class).getBody();
            } catch(Exception e){
                log.error("Excel Generation Error (Sync) : e", e);
                excelResponse.setReqId(request.getReqId());
                excelResponse.setFailed(true);
            } finally {
                updateLocal(excelResponse);
            }
        },
                threadPoolService.getThreadPool());
        //Using CompletableFuture to run the pdf request Async
        CompletableFuture<Void> pdfFuture = CompletableFuture.runAsync(()->{
            PDFResponse pdfResponse = new PDFResponse();
            try {
                pdfResponse = rs.postForEntity("http://localhost:9999/pdf", request, PDFResponse.class).getBody();
            } catch(Exception e){
                log.error("PDF Generation Error (Sync) : e", e);
                pdfResponse.setReqId(request.getReqId());
                pdfResponse.setFailed(true);
            } finally {
                updateLocal(pdfResponse);
            }
        },
                threadPoolService.getThreadPool());
        //block until both tasks are done
        try {
            excelFuture.get();
            pdfFuture.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    //TODO: Combine updateLocal(ExcelResponse) + updateLocal(PDFResponse)
    //once sync requests (pdf+excel) are done, convert requests to SqsResponse objects, then update local repo
    private void updateLocal(FileTypeResponse excelOrPdfResponse) {
        SqsResponse response = new SqsResponse();
        BeanUtils.copyProperties(excelOrPdfResponse, response);
        updateAsyncPDFOrExcelReport(response,
                excelOrPdfResponse instanceof ExcelResponse ? FileType.EXCEL:FileType.PDF);
    }

    //save the request object into local repository, send out the request to SNS, return wrapped object
    @Override
    @Transactional
    public ReportVO generateReportsAsync(ReportRequest request) {
        //parse the ReportRequest to a "local" Datbase Entity
        ReportRequestEntity entity = persistToLocal(request);
        //using SNS service to send out the request to the SNS (simple notification service)
        snsService.sendReportNotification(request);
        //log the step
        log.info("Send SNS the message: {}",request);
        //wrap the ReportRequest object and return it
        return new ReportVO(entity);
    }

    //TODO: Combine updateAsyncPDFReport() + updateAsyncExcelReport()
    //update the local Repo's PDFReportEntity or ExcelReportEntity (of the ReportRequestEntity)
    @Override
    @Transactional // in case of email failure, comment out
    public void updateAsyncPDFOrExcelReport(SqsResponse response, FileType fileType) {
        //retrieve the entity object from repository
        ReportRequestEntity entity = reportRequestRepo.findById(response.getReqId()).orElseThrow(RequestNotFoundException::new);
        //update the PDFReportEntity or ExcelReportEntity (of the ReportRequestEntity)
        var pdfOrExcelReport =
                (fileType==FileType.PDF)? entity.getPdfReport():entity.getExcelReport();
        pdfOrExcelReport.setUpdatedTime(LocalDateTime.now());
        if (response.isFailed()) {
            pdfOrExcelReport.setStatus(ReportStatus.FAILED);
        } else{
            pdfOrExcelReport.setStatus(ReportStatus.COMPLETED);
            pdfOrExcelReport.setFileId(response.getFileId());
            pdfOrExcelReport.setFileLocation(response.getFileLocation());
            pdfOrExcelReport.setFileSize(response.getFileSize());
        }
        //update the ReportRequestEntity update_time
        entity.setUpdatedTime(LocalDateTime.now());
        //store it back to the repository
        reportRequestRepo.save(entity);

        //using emailService to notify user -> specifically, sending the object to email_queue
        String to = "rory323rin@gmail.com";
        emailService.sendEmail(to, EmailType.SUCCESS, entity.getSubmitter());
    }

    @Override
    public void deleteReport(String reqId) {
        RestTemplate rs = new RestTemplate();
        ReportRequestEntity entity = reportRequestRepo.findById(reqId).orElseThrow(RequestNotFoundException::new);
        ExecutorService executorService = threadPoolService.getThreadPool();
        //delete from ExcelService
        executorService.submit(()->{
            ExcelResponse excelResponse = new ExcelResponse();
            try {
                String excelFileId = entity.getExcelReport().getFileId();
                log.info("trying to delete file from excel service: {}",excelFileId);
                excelResponse = rs.exchange("http://localhost:8888/excel/"+excelFileId, HttpMethod.DELETE,
                        null, ExcelResponse.class).getBody();
            } catch(Exception e){
                log.error("Excel Deletion Error : ", e);
            }
        });
        //delete from PDFService
        executorService.submit(()->{
            PDFResponse pdfResponse = new PDFResponse();
            try {
                String PDFFileId = entity.getPdfReport().getFileId();
                log.info("trying to delete file from pdf service: {}",PDFFileId);
                pdfResponse = rs.exchange("http://localhost:9999/pdf/"+PDFFileId, HttpMethod.DELETE,
                        null, PDFResponse.class).getBody();
            } catch(Exception e){
                log.error("PDF Deletion Error : ", e);
            }
        });
        reportRequestRepo.deleteById(reqId);
    }

    @Override
    @Transactional(readOnly = true)
    public List<ReportVO> getReportList() {
        return reportRequestRepo.findAll().stream().map(ReportVO::new).collect(Collectors.toList());
    }

    @Override
    public InputStream getFileBodyByReqId(String reqId, FileType type) {
        ReportRequestEntity entity = reportRequestRepo.findById(reqId).orElseThrow(RequestNotFoundException::new);
        if (type == FileType.PDF) {
            String fileLocation = entity.getPdfReport().getFileLocation(); // this location is s3 "bucket/key"
            String bucket = fileLocation.split("/")[0];
            String key = fileLocation.split("/")[1];
            return s3Client.getObject(bucket, key).getObjectContent();
        } else if (type == FileType.EXCEL) {
            String fileId = entity.getExcelReport().getFileId();
//            String fileLocation = entity.getExcelReport().getFileLocation();
//            try {
//                return new FileInputStream(fileLocation);// this location is in local, definitely sucks
//            } catch (FileNotFoundException e) {
//                log.error("No file found", e);
//            }
            RestTemplate restTemplate = new RestTemplate();
//            InputStream is = restTemplate.execute(, HttpMethod.GET, null, ClientHttpResponse::getBody, fileId);
            ResponseEntity<Resource> exchange = restTemplate.exchange("http://localhost:8888/excel/{id}/content",
                    HttpMethod.GET, null, Resource.class, fileId);
            try {
                return exchange.getBody().getInputStream();
            } catch (IOException e) {
                log.error("Cannot download excel",e);
            }
        }
        return null;
    }
}
