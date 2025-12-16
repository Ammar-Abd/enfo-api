API Structure Overview
Mobile Application APIs

Defined in the plumber.R file

Each API endpoint is clearly separated using #### commented sections

Dashboard APIs

Defined in the consolidated_api.R file

Application Workflow

The api.R file imports and registers APIs from both:

plumber.R

consolidated_api.R

The application is deployed behind Nginx with 4 separate workers, where:

Each worker represents an independent instance of all API endpoints

Each worker maintains its own log file for:

Runtime output

Error tracking and debugging
