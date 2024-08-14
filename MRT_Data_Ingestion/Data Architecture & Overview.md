## Introduction and Requirement

Flow computer inspection reports are uploaded into sharepoint library everyday by rechnicians. These documents consists of PDF and excel forms and specific fields need to be collected for visualizaton. Stakeholders require the collected data to be stored in master excel sheet so they can also perform analysis on the data themselves.

## Data Architecture
![Picture1](https://github.com/user-attachments/assets/bac82a00-b28c-40ee-a294-f0bf0f7e9ebf)

When a technician uploads a sharepoint report, the power automate flow gets triggered and creates the same report in Azure Blob Storage. Azure Functions gets triggered by the creation of blob files and runs the python code to scrape the data and overwrite the master excel file stored in Azure Blob Storage. This master excel file is used to create the Power BI dashboard and generate data visualization.
