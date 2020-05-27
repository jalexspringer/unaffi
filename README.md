# UnAffi
## Cross Affiliate Network Performance and Cost ETL
Airflow ETL pipeline and custom operators using https://github.com/jalexspringer/netimpact package for extraction, storing data in parquet format in S3 for easy Athena access.

## Data Lake Structure

    /[NETWORKNAME]/[PROGRAM]/[YEAR].parquet

    /awin/awin_uk/2020.parquet


## Network Objects

Currently implemented - Awin, Admitad, and Linkshare(Rakuten)

Imports active partners, clicks, transactions and validations from Awin, Admitad, and Linkshare on anywhere from a 30 to 1 minute range, creates partner accounts specifically for this, and then batches transactions as well as reversing/locking anything validated across the same time frame.

Cost comes in by calculating the actual percentage and passing it to Numeric1. Payout groups are used for each percentage point up to 20 - there are no fractions of a percentage point payout in any of the network terms.

Partners are assigned a group for each region/network they belong to (there are many partners joined to multiple), and each transaction is tagged with account region, network, customer country, promo code, customer status, etc. Basically everything the networks could spit out short of product level data.

Handles net vs. gross rev., and treating all of it as direct Impact tracked conversions with no attribution issues.

Also gives us a mapping from partner IDs - I put those in MPValue1.

## Linkshare report requirements

Note the Linkshare reports in each account require a specific set of headers (any order is fine), as the API only allows for pulling existing reports:

    'Consumer Country','Gross Commissions','Gross Sales','Order ID','Publisher ID','Transaction Date','Transaction Time','Process Date','Customer Status', 'Currency'


## Config file example (TOML)

    [Impact]
    impact_sid = 'XXXXXXXXXX'
    impact_token = 'XXXXXXXXXX'
    ftp_un = 'FTP USERNAME'
    ftp_p = 'FTP PASS'
    program_id = 'XXXXX'
    desktop_action_tracker_id = 'XXXXX'
    mobile_action_tracker_id = 'XXXXX'
      
    [Awin]
    oauth = "XXXXXXXXXXXXX"
    [Awin.account_ids]
    'Awin Example 1' = 'XXXX'
    'Awin Example 2' = 'XXXX'

    [Linkshare]
    [Linkshare.report_names]
    'Transactions' = 'transaction-report'
    'Publishers' = 'publisher-report'
    [Linkshare.account_ids]
    'Linkshare AU' = 'XXXXX'
    'Linkshare Asia' = 'XXXXX'
    'Linkshare Concierge' = 'XXXXX'
    'Linkshare UK' = 'XXXXX'
    'Linkshare US' = 'XXXXX'

    [Admitad]
    client_id='XXXXX'
    client_secret='XXXXX'
    account_id = 'XXXX'
    account_name = 'Example_Account'
    [Admitad.account_ids]
    'Example_Account' = 'XXXX'

## Tests
### TODO :: Implement tests

## Usage

For **CeleryExecutor** :

    docker-compose -f docker-compose-CeleryExecutor.yml up -d