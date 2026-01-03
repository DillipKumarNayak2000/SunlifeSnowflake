-- Test cases for the SP_RECON_CLIENT_VIEW stored procedure

-- Test case 1: Valid input parameters
CALL CE_NOVA_DEV.DBO.SP_RECON_CLIENT_VIEW('Portfolio123', '2023-10-01', 'Book Value');

-- Test case 2: Invalid date format
CALL CE_NOVA_DEV.DBO.SP_RECON_CLIENT_VIEW('Portfolio123', '01-10-2023', 'Book Value');

-- Test case 3: Future date
CALL CE_NOVA_DEV.DBO.SP_RECON_CLIENT_VIEW('Portfolio123', '2025-10-01', 'Book Value');

-- Test case 4: Null portfolio number
CALL CE_NOVA_DEV.DBO.SP_RECON_CLIENT_VIEW(NULL, '2023-10-01', 'Book Value');

-- Test case 5: Missing financial field
CALL CE_NOVA_DEV.DBO.SP_RECON_CLIENT_VIEW('Portfolio123', '2023-10-01', NULL);

-- Test case 6: Non-existent portfolio number
CALL CE_NOVA_DEV.DBO.SP_RECON_CLIENT_VIEW('NonExistentPortfolio', '2023-10-01', 'Book Value');