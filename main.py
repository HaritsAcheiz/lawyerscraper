import re
import urllib.parse
from httpx import Client, AsyncClient
from selectolax.parser import HTMLParser
from dataclasses import dataclass
import asyncio
import pandas as pd

limit = asyncio.Semaphore(50)

@dataclass
class LawyerScraper:
    base_url: str = 'https://lsa.memberpro.net'
    user_agent: str = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'

    async def fetch(self, url):
        headers = {
            'user-agent': self.user_agent
        }

        async with AsyncClient(headers=headers, timeout=10) as aclient:
            async with limit:
                response = await aclient.post(url)
                if limit.locked():
                    await asyncio.sleep(1)
                if response.status_code != 200:
                    response.raise_for_status()

                return response.text

    def get_links(self):
        url = urllib.parse.urljoin(self.base_url, '/main/body.cfm?person_nm=&first_nm=&location_nm=&area_ds=Real+Estate+Conveyancing&mode=search')
        headers = {
            'user-agent': self.user_agent
        }
        with Client() as client:
            response = client.post(url, headers=headers)
            if response.status_code != 200:
                response.raise_for_status()
        html = response.text
        return html

    def extract_contacts(self, text):
        # Extract email addresses
        email_pattern = re.compile(r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b")
        emails = email_pattern.findall(text)
        email = ''
        fax = ''
        office = ''
        if len(emails) > 0:
            email = emails[0]

        # Extract fax numbers
        fax_pattern = re.compile(r"Fax\s*\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}")
        faxes = fax_pattern.findall(text)
        if len(faxes) > 0:
            fax = faxes[0].split('  ')[1]

        # Extract office phone numbers
        office_pattern = re.compile(r"Office\s*\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}")
        offices = office_pattern.findall(text)
        if len(offices) > 0:
            office = offices[0].split('  ')[1]

        return email, fax, office

    def get_id(self, href):
        pattern = re.compile(r'javascript:pickStep\((\d+),(\d+)\)')
        match = pattern.match(href)
        if match:
            link = f'https://lsa.memberpro.net/main/body.cfm?menu=directory&submenu=directoryPractisingMember&mode=search&searchField=location_nm&search_value=&record_id={int(match.group(1))}&table_id={int(match.group(2))}'
            return link

    def get_inactive(self, element):
        inactive_lawyer = {'name': '', 'web_cms_email': '', 'web_cms_fax': '', 'web_cms_office': '',
                      'office_phone_number': '', 'fax_phone_number': '', 'gender': '', 'practising_status': '',
                      'enrolment_date': '', 'firm': '', 'address': '', 'practice_area/limited_scope': '',
                      'current_citations': '', 'discipline_history': '', 'notice_reports': ''}
        inactive_lawyer['name'] = element.css_first('td:nth-of-type(1)').text(strip=True)
        inactive_lawyer['address'] = element.css_first('td:nth-of-type(2)').text(strip=True)
        inactive_lawyer['gender'] = element.css_first('td:nth-of-type(3)').text(strip=True)
        inactive_lawyer['practising_status'] = element.css_first('td:nth-of-type(4)').text(strip=True)
        inactive_lawyer['enrolment_date'] = element.css_first('td:nth-of-type(5)').text(strip=True)
        inactive_lawyer['firm'] = element.css_first('td:nth-of-type(6)').text(strip=True)
        inactive_lawyer['practice_area/limited_scope'] = f"Real Estate Conveyancing {element.css_first('td:nth-of-type(2)').text(strip=True)}"
        return inactive_lawyer

    def parse_links(self, html):
        tree = HTMLParser(html)
        elements = tree.css('tbody > TR')
        result = [self.get_id(element.css_first('a').attributes.get('href')) for element in elements if element.css_first('a')]
        inactive_result = [self.get_inactive(element) for element in elements if not element.css_first('a')]
        links = [i for i in result if i is not None]
        return links, inactive_result

    async def get_detail_htmls(self, links):
        tasks = []
        for link in links:
            task = asyncio.create_task(self.fetch(link))
            tasks.append(task)

        detail_htmls = await asyncio.gather(*tasks)

        df = pd.DataFrame(columns=['htmls'], data=detail_htmls)
        df.to_csv('detail_htmls.csv', index=False, quotechar='|')
        return detail_htmls

    def get_practice_area(self, tree):
        practice_areas = []
        practice_area_elems = tree.css('table:nth-of-type(4) > tbody > tr')
        for element in practice_area_elems[1:]:
            try:
                temp_practice_area = element.css_first('td:nth-of-type(1)').text(strip=True) + ' | ' + element.css_first(
                    'td:nth-of-type(2)').text(strip=True)
                practice_areas.append(temp_practice_area)
            except:
                continue
        result = '; '.join(practice_areas)
        return result

    def get_disc_hist(self, tree):
        practice_areas = []
        practice_area_elems = tree.css('table:nth-of-type(6) > tbody > tr')
        for element in practice_area_elems[2:-1]:
            try:
                temp_practice_area = element.css_first('td:nth-of-type(1)').text(strip=True) + ' | ' + element.css_first(
                    'td:nth-of-type(2)').text(strip=True)
                practice_areas.append(temp_practice_area)
            except:
                continue
        result = '; '.join(practice_areas)
        return result

    def get_notice(self, tree):
        practice_areas = []
        practice_area_elems = tree.css('table:nth-of-type(7) > tbody > tr')
        for element in practice_area_elems[2:-1]:
            try:
                temp_practice_area = element.css_first('td:nth-of-type(1)').text(strip=True) + ' | ' + element.css_first(
                    'td:nth-of-type(2)').text(strip=True)
                practice_areas.append(temp_practice_area)
            except:
                continue
        result = '; '.join(practice_areas)
        return result

    def get_citations(self, tree):
        practice_areas = []
        practice_area_elems = tree.css('table:nth-of-type(5) > tbody > tr')
        for element in practice_area_elems[4:-1]:
            try:
                temp_practice_area = element.css_first('td:nth-of-type(1)').text(strip=True) + ' | ' + element.css_first(
                    'td:nth-of-type(2)').text(strip=True)
                practice_areas.append(temp_practice_area)
            except:
                continue
        result = '; '.join(practice_areas)
        return result

    def clean_address(self, address):
        break_address = address.split('\n')
        cleaned_address = [x.strip() for x in break_address]
        result = [i for i in cleaned_address if i != '']
        result = ', '.join(result)
        return result

    def parse_data(self, detail_htmls):
        lawyers = []
        for html in detail_htmls:

            lawyer = {'name': '', 'web_cms_email': '', 'web_cms_fax': '', 'web_cms_office': '',
                      'office_phone_number': '', 'fax_phone_number': '', 'gender': '', 'practising_status': '',
                      'enrolment_date': '', 'firm': '', 'address': '', 'practice_area/limited_scope': '',
                      'current_citations': '', 'discipline_history': '', 'notice_reports': ''}

            tree = HTMLParser(html)
            lawyer['name'] = tree.css_first('div.content-heading').text(strip=True)
            contacts = tree.css_first('div.form-actions').text()
            lawyer['web_cms_email'], lawyer['web_cms_fax'], lawyer['web_cms_office'] = self.extract_contacts(contacts)
            content = tree.css_first('div.content')
            try:
                lawyer['office_phone_number'] = content.css_first('table:nth-of-type(3) > tbody > tr:nth-of-type(2) > td > table > tbody > tr:nth-of-type(2) > td:nth-of-type(2)').text(strip=True)
            except:
                lawyer['office_phone_number'] = ''
            try:
                lawyer['fax_phone_number'] = content.css_first('table:nth-of-type(3) > tbody > tr:nth-of-type(2) > td > table > tbody > tr:nth-of-type(3) > td:nth-of-type(2)').text(strip=True)
            except:
                lawyer['fax_phone_number'] = ''
            try:
                lawyer['gender'] = content.css_first('table:nth-of-type(2) > tbody > tr:nth-of-type(2) > td:nth-of-type(1)').text(strip=True)
            except:
                lawyer['gender'] = ''
            try:
                lawyer['practising_status'] = content.css_first('table:nth-of-type(2) > tbody > tr:nth-of-type(2) > td:nth-of-type(2)').text(strip=True)
            except:
                lawyer['practising_status'] = ''
            try:
                lawyer['enrolment_date'] = content.css_first('table:nth-of-type(2) > tbody > tr:nth-of-type(2) > td:nth-of-type(3)').text(strip=True)
            except:
                lawyer['enrolment_date'] = ''
            try:
                lawyer['firm'] = content.css_first('table:nth-of-type(3) > tbody > tr:nth-of-type(2) > td > table > tbody > tr:nth-of-type(1)').css_first('div').text(strip=True)
            except:
                lawyer['firm'] = ''
            try:
                lawyer['address'] = self.clean_address(content.css_first('table:nth-of-type(3) > tbody > tr:nth-of-type(2) > td > table > tbody > tr:nth-of-type(1) > td').text())
            except:
                lawyer['address'] = ''
            try:
                lawyer['practice_area/limited_scope'] = self.get_practice_area(content)
            except:
                lawyer['practice_area/limited_scope'] = ''
            try:
                lawyer['current_citations'] = self.get_citations(content)
            except:
                lawyer['current_citations'] = ''
            try:
                lawyer['discipline_history'] = self.get_disc_hist(content)
            except:
                lawyer['discipline_history'] = ''
            try:
                lawyer['notice_reports'] = self.get_notice(content)
            except:
                lawyer['notice_reports'] = ''
            lawyers.append(lawyer)
        return lawyers


    def main(self):
        html = self.get_links()
        links, inactive_lawyer = self.parse_links(html)
        detail_htmls = asyncio.run(self.get_detail_htmls(links))
        lawyers = self.parse_data(detail_htmls)
        inactive_df = pd.DataFrame(inactive_lawyer)
        df = pd.DataFrame(lawyers)
        result = pd.concat([df, inactive_df], ignore_index=True)
        result.to_csv('lawyer_data4.csv', index=False)


if __name__ == '__main__':
    scraper = LawyerScraper()
    scraper.main()