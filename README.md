# Antra SEP java evaluation project

## 1 Improvement made
0. Improve sync API performance by using multithreading and sending request concurrently to both services.
1. Simplify methods in ReportServiceImpl by combining methods with similar behaviors:
   
   1.1 updateLocal(ExcelResponse), updateLocal(PDFResponse)
   
   1.2 updateAsyncExcelReport, updateAsyncPDFReport

2. @Delete Method
   
   2.1 Async Calls to both service to delete data and file from both repo and data store
