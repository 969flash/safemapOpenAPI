import requests
import csv
from urllib.parse import urlencode
from xml.etree import ElementTree as ET
import os

##################################################
##################################################
# 해당 코드는 safemap.go.kr의 API를 이용하여 데이터를 가져와 CSV로 변환하는 코드입니다.
# 해당 코드를 실행 시키기 앞서, API 키를 발급받아야 합니다.
# API 키 발급은 https://safemap.go.kr/opna/crtfc/keyAgree.do 에서 진행할 수 있습니다.
# API 키는 신청과 동시에 발급됩니다.
# 2024년12월04일 기준 작성
##################################################
##################################################


# Load API key from external file
def load_api_key(file_path):
    with open(file_path, "r") as file:
        return file.read().strip()


# API 기본 설정
# 도로교통공단 교통사고 다발구역 데이터
base_url = "http://safemap.go.kr/openApiService/data/getTfcacdHspotData.do"
api_key = load_api_key("api_key.txt")  # Load API key from file
params = {
    "serviceKey": api_key,  # Use loaded API key
    "pageNo": "1",
    "numOfRows": "1",  # 최소 요청으로 totalCount를 가져오기
    "dataType": "XML",  # 응답 형식 설정
}


# totalCount 가져오기
def get_total_count():
    url = f"{base_url}?{urlencode(params)}"
    response = requests.get(url)
    if response.status_code == 200:
        try:
            root = ET.fromstring(response.content)
            total_count = root.find(".//totalCount")  # <totalCount> 값 찾기
            if total_count is not None:
                return int(total_count.text)
            else:
                print("Error: Unable to find 'totalCount' in the response.")
                print(response.text)  # 디버깅을 위해 전체 응답 출력
        except ET.ParseError as e:
            print(f"Error parsing XML: {e}")
            print(response.text)  # 디버깅을 위해 전체 응답 출력
    else:
        print(f"Error fetching total count: {response.status_code}")
        print(response.text)  # 디버깅을 위해 전체 응답 출력
    return 0


# 데이터 요청 및 CSV 저장
def fetch_and_save_data():
    total_count = get_total_count()
    if total_count == 0:
        print("No data found or failed to fetch total count.")
        return

    print(f"Total records: {total_count}")
    num_of_rows = 20000  # 최대 20000개씩 요청
    total_pages = (total_count // num_of_rows) + 1  # 전체 페이지 수 계산

    params["numOfRows"] = str(num_of_rows)  # 요청당 행 수 설정

    # CSV 파일 생성
    with open(
        "tfcacd_hspot_data.csv", mode="w", newline="", encoding="utf-8-sig"
    ) as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "OBJT_ID",
                "OCCU_YEAR",
                "OCCU_MT",
                "OCCU_DE",
                "OCCU_TM",
                "OCCU_DAY",
                "OCCU_DATE",
                "LCLAS",
                "SCLAS",
                "DEATH",
                "SWPSN",
                "SINJPSN",
                "INJPSN",
                "BIG",
                "DRNKG",
                "KID",
                "ODSN",
                "WLKG",
                "VIOLT_CN",
                "CTPRVN_CD",
                "SGG_CD",
                "EMD_CD",
                "X",
                "Y",
            ]
        )  # 필요한 열 설정

        for page in range(1, total_pages + 1):
            params["pageNo"] = str(page)
            url = f"{base_url}?{urlencode(params)}"
            response = requests.get(url)
            if response.status_code == 200:
                try:
                    root = ET.fromstring(response.content)
                    for item in root.findall(".//item"):  # XML 데이터에서 item 찾기
                        row = [
                            item.findtext("OBJT_ID", default=""),
                            item.findtext("OCCU_YEAR", default=""),
                            item.findtext("OCCU_MT", default=""),
                            item.findtext("OCCU_DE", default=""),
                            item.findtext("OCCU_TM", default=""),
                            item.findtext("OCCU_DAY", default=""),
                            item.findtext("OCCU_DATE", default=""),
                            item.findtext("LCLAS", default=""),
                            item.findtext("SCLAS", default=""),
                            item.findtext("DEATH", default=""),
                            item.findtext("SWPSN", default=""),
                            item.findtext("SINJPSN", default=""),
                            item.findtext("INJPSN", default=""),
                            item.findtext("BIG", default=""),
                            item.findtext("DRNKG", default=""),
                            item.findtext("KID", default=""),
                            item.findtext("ODSN", default=""),
                            item.findtext("WLKG", default=""),
                            item.findtext("VIOLT_CN", default=""),
                            item.findtext("CTPRVN_CD", default=""),
                            item.findtext("SGG_CD", default=""),
                            item.findtext("EMD_CD", default=""),
                            item.findtext("X", default=""),
                            item.findtext("Y", default=""),
                        ]
                        writer.writerow(row)
                except ET.ParseError as e:
                    print(f"Error parsing XML on page {page}: {e}")
                    print(response.text)  # 디버깅을 위해 전체 응답 출력
            else:
                print(f"Error fetching page {page}: {response.status_code}")
                break
            print(f"Page {page}/{total_pages} fetched and saved.")


# 실행
fetch_and_save_data()
