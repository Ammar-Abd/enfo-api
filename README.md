## API Structure Overview

### Mobile Application APIs
- Defined in the `plumber.R` file  
- Each API endpoint is separated using `####` commented sections

### Dashboard APIs
- Defined in the `consolidated_api.R` file

---

## Application Workflow

- The `api.R` file imports and registers APIs from:
  - `plumber.R`
  - `consolidated_api.R`

- The application is deployed behind **Nginx** with **4 separate workers**:
  - Each worker is an independent instance of all API endpoints
  - Each worker maintains its own log file for:
    - Runtime output
    - Error diagnostics
  - Additionally, nginx is set up to act as a load balancer to ensure that the load between the APIs is equally divided
