# Automated pulling of symbols from
# puts all relevant symbols by exchange and security type into requisite folder

import logging
from pathlib import Path
from time import sleep, perf_counter
import pandas as pd

from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from bs4 import BeautifulSoup


def get_browser_log_entries(driver):
    """get log entries from selenium and add to python logger before returning"""
    loglevels = {
        "NOTSET": 0,
        "DEBUG": 10,
        "INFO": 20,
        "WARNING": 30,
        "ERROR": 40,
        "SEVERE": 40,
        "CRITICAL": 50,
    }

    # initialise a logger
    browserlog = logging.getLogger("chrome")
    # get browser logs
    slurped_logs = driver.get_log("browser")
    for entry in slurped_logs:
        rec = browserlog.makeRecord(
            "%s.%s" % (browserlog.name, entry["source"]),
            loglevels.get(entry["level"]),
            ".",
            0,
            entry["message"],
            None,
            None,
        )
        # log using original timestamp.. us -> ms
        rec.created = entry["timestamp"] / 1000
        try:
            # add browser log to python log
            browserlog.handle(rec)
        except:
            print(entry)
    # and return logs incase you want them
    return slurped_logs


initial_tic = perf_counter()

# instance of Options class allows
# us to configure Headless Chrome
options = Options()
options.add_experimental_option("excludeSwitches", ["enable-logging"])

# this parameter tells Chrome that
# it should be run without UI (Headless)
options.headless = False

caps = webdriver.DesiredCapabilities.CHROME.copy()
caps["goog:loggingPrefs"] = {"browser": "ALL"}

driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    desired_capabilities=caps,
    options=options,
)

driver.get("https://ws1.dtn.com/IQ/Search/")

sleep(1.5)

# # add the ability to Jquery
# driver.execute_script("""var jquery_script = document.createElement('script');
# jquery_script.src = 'https://ajax.googleapis.com/ajax/libs/jquery/1.7.1/jquery.min.js';
# jquery_script.onload = function(){var $ = window.jQuery;};
# document.getElementsByTagName('head')[0].appendChild(jquery_script);""")

driver.find_element("id", "htmlTable").click()

# eventually could be command line options
no_option_or_spread = True
only_option_or_spread = False
restart_from_exchange = False
restart_at_exchange = "ENID"


if no_option_or_spread:
    driver.find_element("id", "noOptions").click()
    driver.find_element("id", "noSpreads").click()

sleep(1.5)

exchanges = driver.find_element("id", "exchangeSelect").text
exchanges_list = exchanges.split("\n")
exchanges_list[0] = "ALL"
exchanges_list.remove("ALL")

security_types = driver.find_element("id", "securityTypeSelect").text
security_types_list = security_types.split("\n")
security_types_list[0] = "ALL"
security_types_list.remove("ALL")

if only_option_or_spread:
    security_types_processed_list = []
    for security in security_types_list:
        if "option" in security.lower() or "spread" in security.lower():
            security_types_processed_list.append(security)
    security_types_list = security_types_processed_list

if restart_from_exchange:
    lastExchangeIndex = exchanges_list.index(restart_at_exchange)
    exchanges_list = exchanges_list[lastExchangeIndex::]

print(exchanges_list)
print(security_types_list)

output_columns = [
    "Symbol",
    "Description",
    "Security Type",
    "Exchange",
    "Listed Market",
    "Exchange",
    "Security",
]

# change for your needs
base_dir_path = ".../iqfeed-symbols"
base_dir = Path(base_dir_path)


if only_option_or_spread:
    base_dir = Path(base_dir_path + "-options")

# combined_symbol_df = pd.DataFrame()

# exchanges_list = ["NASDAQ", "NYSE", "CME"]
# security_types_list = ["Equity"]

selectExchange = Select(driver.find_element("id", "exchangeSelect"))
selectSecurity = Select(driver.find_element("id", "securityTypeSelect"))

checkForData = True
previousTimestamp = 0


try:
    for exchange in exchanges_list:
        # if (exchange == "CME"):
        #     driver.find_element("id","miniOnly").click()
        selectExchange.select_by_value(exchange)
        # driver.find_element_by_xpath('option[@value="%s"]' % exchange).click()
        # driver.find_element_by_link_text(exchange).click()
        # browser.find_option_by_text(exchange).click()
        # current_exchange = driver.find_element_by_link_text(exchange).text

        exchange_processed = (
            exchange.replace(" ", "-").replace("/", "-").replace("_", "-")
        )
        exchange_lower_case = exchange_processed.lower()

        for security_type in security_types_list:
            selectSecurity.select_by_visible_text(security_type)

            security_type_processed = (
                security_type.replace(" ", "-").replace("/", "-").replace("_", "-")
            )
            security_type_lower_case = security_type_processed.lower()

            print(f"Exchange is: {exchange}, Security is: {security_type}")

            driver.find_element("id", "searchButton").click()
            sleep(0.4)

            consolemsgs = get_browser_log_entries(driver)

            count = 0
            checkForData = True
            waitForData = False
            while checkForData:
                if len(consolemsgs) == 0 or waitForData:
                    count += 1
                    sleep(0.3)
                    consolemsgs = get_browser_log_entries(driver)
                if len(consolemsgs) != 0:
                    for d in consolemsgs:
                        message = d["message"]
                        if message:
                            if "Data!" in message:
                                currentTimestamp = d["timestamp"]
                                if currentTimestamp:
                                    if currentTimestamp != previousTimestamp:
                                        checkForData = False
                            else:
                                waitForData = True
                if count > 20:
                    raise Exception(
                        "Exception waiting for data - need to restart from listed exchange"
                    )

            previousTimestamp = currentTimestamp

            display_string = driver.find_element("id", "quantityHeader").text

            if display_string != "No records found":
                total_records = int(display_string.split()[4])
            else:
                continue

            total_pages = total_records // 250
            current_page = 0

            print("total pages is: ", total_pages)

            df = pd.DataFrame()

            tic = perf_counter()
            parsedMoreThanTotalRecords = False
            not_complete = True
            while not_complete:
                df2 = pd.DataFrame()

                soup = BeautifulSoup(
                    driver.execute_script("return document.body.innerHTML;"),
                    "html.parser",
                )
                tbl = soup.find("table", {"id": "symbolTable"})
                df2 = pd.read_html(str(tbl))[0]
                df2["ExchangeType"] = exchange_lower_case
                df2["SecurityType"] = security_type_lower_case

                df = pd.concat([df, df2], ignore_index=True)
                # combined_symbol_df = pd.concat([combined_symbol_df, df], ignore_index=True)

                noMoreRecords = False
                c = driver.find_element("id", "nextSpanTop").get_attribute("class")
                if c == "d-none":
                    noMoreRecords = True

                # button exists to iterate through more pages
                if not noMoreRecords:
                    current_page += 1
                    if current_page % 10 == 0:
                        print(current_page)

                    driver.find_element("id", "nextButtonTop").click()
                    sleep(0.4)

                    consolemsgs = get_browser_log_entries(driver)

                    count = 0
                    checkForData = True
                    waitForData = False
                    while checkForData:
                        if len(consolemsgs) == 0 or waitForData:
                            count += 1
                            sleep(0.3)
                            consolemsgs = get_browser_log_entries(driver)
                        if len(consolemsgs) != 0:
                            for d in consolemsgs:
                                message = d["message"]
                                if message:
                                    if "Data!" in message:
                                        currentTimestamp = d["timestamp"]
                                        if currentTimestamp:
                                            if currentTimestamp != previousTimestamp:
                                                checkForData = False
                                    else:
                                        waitForData = True
                        if count > 20:
                            raise Exception(
                                "Exception waiting for data - need to restart from listed exchange"
                            )

                    previousTimestamp = currentTimestamp

                    if (df.shape[0] / total_records) > 1.0:
                        parsedMoreThanTotalRecords = True
                else:
                    if current_page != total_pages:
                        print(
                            "Early stop criteria hit, Cannot access more records / no next button"
                        )
                    not_complete = False

            toc = perf_counter()
            print(f"Took {toc - tic:0.4f} seconds")
            if parsedMoreThanTotalRecords:
                print(
                    "Informational Only - \
                    Total records exceeded but iterated through more records than exists",
                    df.shape,
                    " ",
                    df.shape[0] / total_records,
                    exchange,
                    security_type,
                )

            output_dir = base_dir / exchange_lower_case / security_type_lower_case

            if not Path.exists(output_dir):
                Path.mkdir(output_dir, parents=True, exist_ok=True)

            # output_dir_temp = output_dir + "/"
            filename = (
                exchange_lower_case + "-" + security_type_lower_case + "-symbols.csv"
            )
            df.to_csv(output_dir / filename, header=True, index=False)
except Exception as e:
    print("Exception hit ", e)
    driver.quit()

end_tic = perf_counter()
print(f"Took {initial_tic - end_tic:0.4f} seconds overall")
