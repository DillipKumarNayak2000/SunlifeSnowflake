# Project Overview

This project contains a set of Snowflake stored procedures and views designed for financial data reconciliation. The procedures handle various aspects of Sap ,Pam data retrieval and aggregation, while the views provide a structured way to access this data.

## Directory Structure

- **procedures/**: Contains the SQL files for stored procedures.
  - `sp_recon_client_view.sql`: Stored procedure for client view.
  - `sp_recon_account_view.sql`: Stored procedure for account view.
  - `sp_recon_portfolio_summary.sql`: Stored procedure for portfolio summary view.

- **views/**: Contains the SQL files for views.
  - `v_recon_client.sql`: SQL view for client data.
  - `v_recon_account.sql`: SQL view for account data.
  - `v_recon_portfolio_summary.sql`: SQL view for portfolio summary data.

- **migrations/**: Contains SQL migration files.
  - `001_create_procedures.sql`: SQL commands to create stored procedures.
  - `002_grants.sql`: SQL commands to grant necessary permissions.

- **tests/**: Contains SQL files for testing stored procedures.
  - `test_sp_recon_client_view.sql`: Test cases for the client view stored procedure.
  - `test_sp_recon_account_view.sql`: Test cases for the account view stored procedure.
  - `test_sp_recon_portfolio_summary.sql`: Test cases for the portfolio summary stored procedure.

- **scripts/**: Contains shell scripts for deployment and testing.
  - `deploy.sh`: Script for deploying stored procedures and views.
  - `run_tests.sh`: Script for executing test cases.

## Setup Instructions

1. **Clone the Repository**: 
   Clone this repository to your local machine using:
   ```
   git clone <repository-url>
   ```

2. **Install Dependencies**: 
   Ensure you have the necessary dependencies installed for Snowflake and Python.

3. **Run Migrations**: 
   Execute the migration scripts to create the stored procedures and grant permissions:
   ```
   snowflake -f migrations/001_create_procedures.sql
   snowflake -f migrations/002_grants.sql
   ```

4. **Deploy Procedures and Views**: 
   Use the deploy script to deploy the procedures and views:
   ```
   ./scripts/deploy.sh
   ```

5. **Run Tests**: 
   Execute the test cases to validate the functionality of the stored procedures:
   ```
   ./scripts/run_tests.sh
   ```

## Usage Guidelines

- Call the stored procedures with the appropriate parameters to retrieve or aggregate financial data.
- Use the views for direct access to the underlying data structures.
- Ensure to run tests after making any changes to the procedures or views to maintain data integrity.

## Contributing

Contributions are welcome! Please submit a pull request or open an issue for any enhancements or bug fixes.
