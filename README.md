# Azure_ETL

# Azure ETL Pipeline Project

## Project Overview
This project demonstrates the creation of an ETL pipeline on Azure, leveraging various Azure services to build a robust data pipeline. The pipeline ingests data from an HTTP server (GitHub), performs transformations using Azure Databricks, and stores the transformed data in an Azure Data Lake Storage Gen2 (ADLS Gen2) account. 

### Key Technologies Used:
- **Azure Data Lake Storage Gen2 (ADLS Gen2)**: Hierarchical storage account for storing data.
- **Azure Data Factory (ADF)**: To build the data ingestion pipeline from an HTTP server to ADLS Gen2.
- **Azure Databricks**: For performing data transformations and returning the results back to ADLS Gen2.
- **Azure App Registration**: Used to securely connect Databricks to ADLS Gen2.

---

## Architecture Overview
1. **Data Ingestion using Azure Data Factory (ADF)**:
   - The pipeline in Azure Data Factory ingests data from a remote HTTP server (GitHub) and loads it into ADLS Gen2.
   - **Dynamic Copy** is used for flexible data copying, utilizing parameters defined in a JSON file.
   - The **Lookup Activity** is used to dynamically load these parameters and use them in the "ForEach" iteration to process data in batches.

2. **Transformations with Azure Databricks**:
   - The ADLS Gen2 storage account is connected to Azure Databricks via **App Registration**.
   - The data is mounted to Databricks, allowing for transformations using Spark.
   - After processing, the transformed data is written back to ADLS Gen2 storage.

3. **Storage in ADLS Gen2**:
   - All raw and transformed data is stored securely in the hierarchical ADLS Gen2 storage account.

---

## Pipeline Flow
1. **Data Ingestion**:
   - Data is pulled from an HTTP server (e.g., GitHub).
   - The data is loaded into an Azure Data Lake Storage Gen2 container using **Azure Data Factory's Dynamic Copy** activity.
   - The parameters, including source URL, destination path, and other configuration settings, are fetched from a **JSON file** using the **Lookup method** in ADF.

2. **Processing and Transformation**:
   - The data is mounted into **Azure Databricks** using an **App Registration** to provide secure access.
   - Transformations are applied on the data using Databricks' powerful Spark-based engine.
   - After transformations, the resulting data is stored back in ADLS Gen2.

3. **Final Storage**:
   - The processed and transformed data is returned to ADLS Gen2 for further analysis or consumption.

---

## Steps to Set Up the ETL Pipeline

### 1. **Create an Azure Data Lake Storage Gen2 Account**
   - Set up a hierarchical storage account on Azure.
   - Create containers for raw and processed data.

### 2. **Set Up Azure Data Factory**
   - Create an Azure Data Factory instance.
   - Define a **Pipeline** that includes:
     - **HTTP Dataset**: To ingest data from the GitHub HTTP server.
     - **Copy Activity**: Using dynamic parameters for flexible copy operations.
     - **ForEach Activity**: To iterate over multiple items and perform data ingestion.

### 3. **Create JSON File with Parameters**
   - Define your dynamic parameters in a JSON file, which will be used by ADF during the pipeline execution.
   - Example JSON:
   ```json
   {
     "sourceUrl": "https://github.com/some-data-repo",
     "destinationPath": "adlsgen2-container/raw-data"
   }
   ```

### 4. **Set Up Azure Databricks**
   - Create an **Azure Databricks workspace**.
   - Register an **App Registration** in Azure Active Directory (AAD) to provide secure access to ADLS Gen2 from Databricks.
   - Mount the ADLS Gen2 storage in Databricks using the App Registration credentials.

### 5. **Perform Data Transformations in Databricks**
   - Once data is mounted, perform the necessary transformations in Databricks using Spark.
   - After processing the data, write the results back to ADLS Gen2.

### 6. **Test the Pipeline**
   - Execute the pipeline to ensure that data flows seamlessly from the HTTP server to ADLS Gen2, gets processed in Databricks, and is returned to ADLS Gen2.

---

## How to Run
1. Clone the repository to your local machine.
2. Set up an Azure Data Lake Storage Gen2 account and Data Factory as described above.
3. Follow the configuration steps to set up the ADF pipeline and Azure Databricks.
4. Run the ADF pipeline to start the data ingestion process.
5. Monitor the pipeline in ADF and Databricks for successful execution and transformations.

---

## Conclusion
This project demonstrates the end-to-end process of creating an ETL pipeline using Azure services. By leveraging **Azure Data Factory**, **Azure Databricks**, and **Azure Data Lake Storage Gen2**, we can efficiently process and store large datasets in the cloud.

---

## Future Enhancements
- Add logging and monitoring to track pipeline execution.
- Implement error handling and retry mechanisms in ADF pipeline.
- Add automated tests for data validation.
