import requests
from bs4 import BeautifulSoup
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe

all_countries_codes = ["ITA", "AUT", "GRC", "NLD", "DEU", "BEL"]
deaths_indicators = ["Number of deaths", "Number of infant deaths", "Number of under-five deaths",
                     "Mortality rate for 5-14 year-olds (probability of dying per 1000 children aged 5-14 years)",
                     "Adult mortality rate (probability of dying between 15 and 60 years per 1000 population)",
                     "Estimates of number of homicides", "Crude suicide rates (per 100 000 population)",
                     "Mortality rate attributed to unintentional poisoning (per 100 000 population)",
                     "Number of deaths attributed to non-communicable diseases, by type of disease and sex",
                     "Estimated road traffic death rate (per 100 000 population)", "Estimated number of road traffic "
                                                                                   "deaths"]

weight_indicators = ["Mean BMI (kg/m&#xb2;) (crude estimate)", "Mean BMI (kg/m&#xb2;) (age-standardized estimate)",
                     "Prevalence of obesity among adults, BMI &GreaterEqual; 30 (crude estimate) (%)",
                     "Prevalence of obesity among children and adolescents, BMI > +2 standard deviations above the "
                     "median (crude estimate) (%)", "Prevalence of overweight among adults, BMI &GreaterEqual; 25 "
                                                    "(age-standardized estimate) (%)",
                     "Prevalence of overweight among children and adolescents, BMI > +1 standard deviations above "
                     "the median (crude estimate) (%)", "Prevalence of underweight among adults, BMI < 18.5 "
                                                        "(age-standardized estimate) (%)",
                     "Prevalence of thinness among children and adolescents, BMI < -2 standard deviations below the "
                     "median (crude estimate) (%)"]

other_indicators = ["Alcohol, recorded per capita (15+) consumption (in litres of pure alcohol)",
                    "Estimate of daily cigarette smoking prevalence (%)",
                    "Estimate of daily tobacco smoking prevalence (%)", "Estimate of current cigarette smoking "
                                                                        "prevalence (%)",
                    "Estimate of current tobacco smoking prevalence (%)",
                    "Mean systolic blood pressure (crude estimate)",
                    "Mean fasting blood glucose (mmol/l) (crude estimate)", "Mean Total Cholesterol (crude estimate)"]

all_indicators = deaths_indicators + weight_indicators + other_indicators


def get_data_from_country(country_code):  # API GET REQUEST TO GET ALL DATA FROM 1 COUNTRY
    response = requests.get("http://tarea-4.2021-1.tallerdeintegracion.cl/gho_" + str(country_code) + ".xml")
    return response


def filtrate_data_from_country(response, indicators):  # ALL DATA FROM COUNTRY IS FILTERED BY THE DESIRED INDICATORS
    indicator_data = []
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "xml")
        facts = soup.find_all('Fact')
        for fact in facts:
            gho = fact.find("GHO").get_text()
            if gho in set(indicators):
                country = fact.find("COUNTRY").get_text() if fact.find("COUNTRY") else None
                sex = fact.find("SEX").get_text() if fact.find("SEX") else None
                year = fact.find("YEAR").get_text() if fact.find("YEAR") else None
                ghecauses = fact.find("GHECAUSES").get_text() if fact.find("GHECAUSES") else None
                agegroup = fact.find("AGEGROUP").get_text() if fact.find("AGEGROUP") else None
                display = fact.find("Display").get_text() if fact.find("Display") else None
                numeric = float(fact.find("Numeric").get_text()) if fact.find("Numeric") else None
                high = float(fact.find("High").get_text()) if fact.find("High") else None
                low = float(fact.find("Low").get_text()) if fact.find("Low") else None
                data = [gho, country, sex, year, ghecauses, agegroup, display, numeric, high, low]
                indicator_data.append(data)
    else:
        response.raise_for_status()

    return indicator_data


def data_to_pandas_dataframe(indicator_data):  # ALL FILTERED DATA TO A DATAFRAME
    df = pd.DataFrame(indicator_data, columns=["GHO", "COUNTRY", "SEX", "YEAR", "GHECAUSES", "AGEGROUP", "DISPLAY",
                                               "Numeric", "High", "Low"])
    return df


def create_dataframe_with_all_data_from_all_countries_by_group_of_indicators(countries_codes,
                                                                             indicators):  # CREATE A DATAFRAME
    # WITH ALL DATA FILTERED BY THE DESIRED INDICATOR FROM ALL 6 COUNTRIES
    all_info_dataframe = pd.DataFrame(columns=["GHO", "COUNTRY", "SEX", "YEAR", "GHECAUSES", "AGEGROUP", "DISPLAY",
                                               "Numeric", "High", "Low"])

    for code in countries_codes:
        response = get_data_from_country(code)
        indicators_data = filtrate_data_from_country(response, indicators)
        indicators_dataframe = data_to_pandas_dataframe(indicators_data)
        all_info_dataframe = all_info_dataframe.append(indicators_dataframe)
    return all_info_dataframe


def dataframe_to_google_spreadsheets(all_data_dataframe):
    gc = gspread.service_account(filename="t4-iic3103-316701-b81a719a8138.json")
    sh = gc.open_by_key("1do07jepQY8QrUWqKYrEZv3zXwxu8utZnGvp_gX74pJY")
    worksheet = sh.get_worksheet(0)
    set_with_dataframe(worksheet, all_data_dataframe)


dataframe = create_dataframe_with_all_data_from_all_countries_by_group_of_indicators(all_countries_codes,
                                                                                     all_indicators)
dataframe_to_google_spreadsheets(dataframe)
