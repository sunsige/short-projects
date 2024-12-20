import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
from datetime import datetime, timedelta

# logging config
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# start and end dates
start_date = '2022-01-01'
end_date = '2022-12-31'

# initialisation
data_records = []
start_date_dt = datetime.strptime(start_date, '%Y-%m-%d')
end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')
current_date_dt = start_date_dt

while current_date_dt <= end_date_dt:
    date_str = current_date_dt.strftime('%Y%m%d')
    logging.info(f"Fetching data for date: {date_str}")

    file_index = 1
    has_data = True

    while has_data:
        url = f"https://www.treasurydirect.gov/xml/R_{date_str}_{file_index}.xml"
        logging.info(f"Requesting URL: {url}")

        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            logging.error(f"No data found for {date_str}_{file_index}.")
            has_data = False
            break

        try:
            # parse XML
            soup = BeautifulSoup(response.content, 'xml')
            logging.info(f"Parsing data for URL: {url}")

            # find AuctionAnnouncement and AuctionResults to extract data
            announcement = soup.find('AuctionAnnouncement')
            results = soup.find('AuctionResults')

            if announcement and results:
                record = {
                    'Date': current_date_dt.strftime('%Y-%m-%d'),
                    'SecurityTerm': announcement.find('SecurityTermWeekYear').text if announcement.find('SecurityTermWeekYear') else '',
                    'SecurityType': announcement.find('SecurityType').text if announcement.find('SecurityType') else '',
                    'CUSIP': announcement.find('CUSIP').text if announcement.find('CUSIP') else '',
                    'AnnouncementDate': announcement.find('AnnouncementDate').text if announcement.find('AnnouncementDate') else '',
                    'AuctionDate': announcement.find('AuctionDate').text if announcement.find('AuctionDate') else '',
                    'IssueDate': announcement.find('IssueDate').text if announcement.find('IssueDate') else '',
                    'MaturityDate': announcement.find('MaturityDate').text if announcement.find('MaturityDate') else '',
                    'OfferingAmount': announcement.find('OfferingAmount').text if announcement.find('OfferingAmount') else '',
                    'BidToCoverRatio': results.find('BidToCoverRatio').text if results.find('BidToCoverRatio') else '',
                    'HighYield': results.find('HighYield').text if results.find('HighYield') else '',
                    'LowYield': results.find('LowYield').text if results.find('LowYield') else '',
                    'MedianYield': results.find('MedianYield').text if results.find('MedianYield') else '',
                    'HighPrice': results.find('HighPrice').text if results.find('HighPrice') else '',
                    'AmountAcceptedBelowLowRate': results.find('AmountAcceptedBelowLowRate').text if results.find('AmountAcceptedBelowLowRate') else '',
                    'AccruedInterest': results.find('AccruedInterest').text if results.find('AccruedInterest') else '',
                    'TreasuryDirectAccepted': results.find('TreasuryDirectAccepted').text if results.find('TreasuryDirectAccepted') else ''
                }
                data_records.append(record)
                
            else:
                logging.warning(f"No Auction data found for URL: {url}")
            
            has_data = True
            file_index += 1

        except requests.exceptions.RequestException as e:
            logging.error(f"Error parsing data for URL {url}: {e}")

    # exit loop if no data found for the current date and move on to the next day
    current_date_dt += timedelta(days=1)

# as required, create pandas dataframe
data = pd.DataFrame(data_records)

# save as CSV
output_file = 'treasury_auction_results.csv'
data.to_csv(output_file, index=False)
logging.info(f"Data saved to {output_file}")