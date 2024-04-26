# LinkedIn Data Analytics Project

## About the Project

This project is designed to query the LinkedIn API to collect data related to my personal activity on LinkedIn. The goal is to store, transform, and analyze this data to gain insights into engagement, conversations, network interactions. This process could be used to enhance content and interaction strategies on LinkedIn based on data-driven analysis. 
More data domains are available on the Linkedin Member Snapshot API as you can see [here](https://learn.microsoft.com/en-us/linkedin/dma/member-data-portability/shared/snapshot-domain).

## Features

- **Data Collection:** Utilizing the LinkedIn API to fetch relevant data about user activity.
- **Data Transformation & Storage:** Cleaning and restructuring data to prepare for analysis. Then stored in a structured way using local MySQL databases to facilitate easy and fast analysis.
- **Data Analysis:** Applying statistical techniques and data visualization to interpret the data and derive useful insights.

## Technologies Used

- **LinkedIn Member Snapshot API:** For collecting data directly from LinkedIn [API documentation](https://learn.microsoft.com/en-us/linkedin/dma/member-data-portability/member-data-portability-member/).
- **Python:** 
  - **Pandas:** For data manipulation.
  - **Matplotlib/Seaborn:** For data visualization.
- **MySQL:** Database management system for data storage.
- **GitHub Secrets:** Used to securely manage environment variables and API keys.

## Project Structure

```plaintext
/
├── fetch-linkedin-api/                  # Python script to request linkedin Member Snapshot API
├── etl-linkedin-data/               # Python scripts for data collection and transformation
├── linkedin-data-analysis/             # Data exploration and preliminary analyses
└── README.md
```

## Installation and Setup

To get this project up and running on your own system:

- Clone the repository.

- Install dependencies:

    ```pip install -r requirements.txt```

- [Set up API keys](https://learn.microsoft.com/en-us/linkedin/dma/member-data-portability/member-data-portability-member/): Store your API keys and other sensitive configurations in a .env file using GitHub Secrets for security.


## Contributions
Contributions to this project are welcome.

## Contact
Martin Founeau – martinfouneau.data@gmail.com