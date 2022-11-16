if __name__ == '__main__':
    import undetected_chromedriver as uc
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import Select
    import time
    import json

    pages = 1


    def process_log(log, file_out):
        log = json.loads(log["message"])["message"]
        if "Network.responseReceived" == log["method"] and "params" in log.keys() and "application/json" in \
                log["params"]["response"]['mimeType']:
            if "GetHistoricSpills" in log["params"]["response"]['url'] and "activity=Genuine" not in log["params"] \
                    ["response"]['url']:
                json_object = json.loads(
                    driver.execute_cdp_cmd('Network.getResponseBody', {'requestId': log["params"]["requestId"]})[
                        "body"])
                items = (json_object["items"])
                json_string = json.dumps(items)
                file_out.write(json_string)
                print(log["params"]["response"]['url'])
                print(driver.execute_cdp_cmd('Network.getResponseBody', {'requestId': log["params"]["requestId"]})[
                          "body"])


    def get_no_pages(log):
        log = json.loads(log["message"])["message"]
        if "Network.responseReceived" == log["method"] and "params" in log.keys() and "application/json" in \
                log["params"]["response"]['mimeType']:
            if "GetHistoricSpills" in log["params"]["response"]['url'] and "activity=Genuine" not in \
                    log["params"]["response"]['url']:
                json_object = json.loads(
                    driver.execute_cdp_cmd('Network.getResponseBody', {'requestId': log["params"]["requestId"]})[
                        "body"])
                global pages
                pages = (json_object["totalPages"])
                return True


    options = webdriver.ChromeOptions()
    options.headless = False
    options.set_capability("goog:loggingPrefs", {"performance": "ALL", "browser": "ALL"})
    driver = uc.Chrome(options=options)
    driver.maximize_window()
    driver.get('https://www.southernwater.co.uk/water-for-life/our-bathing-waters/beachbuoy')
    time.sleep(15)
    file = open("test.json", "w")
    driver.execute_script("arguments[0].scrollIntoView();", driver.find_element(By.ID, 'ddlActivity'))
    releaseType = Select(driver.find_element(By.ID, 'ddlActivity'))
    releaseType.select_by_visible_text("All Releases")
    driver.execute_script("arguments[0].scrollIntoView();", driver.find_element(By.ID, 'btnHistoricSpillsDetails'))
    driver.find_element(By.ID, 'btnHistoricSpillsDetails').click()
    time.sleep(10)
    log_entries = driver.get_log("performance")
    for entry in log_entries:

        try:
            obj_serialized: str = entry.get("message")
            HistoricSpills = get_no_pages(entry)
            if HistoricSpills:
                process_log(entry, file)
            # file.write(obj_serialized + ",")
        except Exception as e:
            raise e from None

    print(pages)
    for i in range(pages):
        while True:
            try:
                driver.execute_script("arguments[0].scrollIntoView();", driver.find_element(By.ID, 'btnNext'))
                driver.find_element(By.ID, 'btnNext').click()
                time.sleep(1)
            except:
                time.sleep(1)
                continue
            break

    log_entries = driver.get_log("performance")
    time.sleep(10)
    for entry in log_entries:

        try:
            obj_serialized: str = entry.get("message")
            process_log(entry, file)
            # file.write(obj_serialized + ",")
        except Exception as e:
            raise e from None

    file.close()
    time.sleep(100)
    # driver.get('https://www.southernwater.co.uk/gateway/Beachbuoy/1.0/api/v1.0/Spills/GetAllAreas')
    # driver.quit()
    # print("Program Ended")
