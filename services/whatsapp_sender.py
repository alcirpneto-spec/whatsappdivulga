import logging
import time

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

from config import CHROME_BINARY_PATH, CHROME_DRIVER_PATH, GROUP_NAME, WHATSAPP_PROFILE_DIR


class WhatsAppSender:
    def __init__(self, driver_path=CHROME_DRIVER_PATH, chrome_binary_path=CHROME_BINARY_PATH):
        self.driver_path = driver_path
        self.chrome_binary_path = chrome_binary_path

    def _build_driver(self):
        service = Service(self.driver_path)
        options = webdriver.ChromeOptions()
        options.add_argument(f'--user-data-dir={WHATSAPP_PROFILE_DIR}')
        options.add_argument('--profile-directory=Default')
        options.add_argument('--disable-notifications')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')

        if self.chrome_binary_path:
            options.binary_location = self.chrome_binary_path

        return webdriver.Chrome(service=service, options=options)

    def send_group_message(self, message):
        if not GROUP_NAME or GROUP_NAME == 'Nome do Grupo':
            raise ValueError('Defina GROUP_NAME em config.py antes de usar a automação WhatsApp.')

        logging.info('Abrindo WhatsApp Web...')
        driver = self._build_driver()
        driver.get('https://web.whatsapp.com')

        logging.info('Aguardando login no WhatsApp Web...')
        time.sleep(12)

        try:
            self._search_group(driver, GROUP_NAME)
            self._send_message(driver, message)
            logging.info('Mensagem enviada com sucesso ao grupo.')
        except Exception as exc:
            logging.error(f'Falha ao enviar mensagem: {exc}')
        finally:
            time.sleep(3)
            driver.quit()

    def _search_group(self, driver, group_name):
        search_selector = "//div[@contenteditable='true' and @data-tab='3']"
        search_box = driver.find_element(By.XPATH, search_selector)
        search_box.click()
        time.sleep(1)
        search_box.clear()
        search_box.send_keys(group_name)
        time.sleep(3)

        title_selector = f"//span[@title='{group_name}']"
        group_title = driver.find_element(By.XPATH, title_selector)
        group_title.click()
        time.sleep(2)

    def _send_message(self, driver, text):
        input_selector = "//div[@contenteditable='true' and @data-tab='10']"
        message_box = driver.find_element(By.XPATH, input_selector)
        message_box.click()
        time.sleep(1)
        for line in text.split('\n'):
            message_box.send_keys(line)
            message_box.send_keys(Keys.SHIFT, Keys.ENTER)
        message_box.send_keys(Keys.ENTER)
        time.sleep(2)
