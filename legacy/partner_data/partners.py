from .extracting import read_csv_from_bytes, read_excel_from_bytes, insert_dataframe, get_email_wrapper
import pandas as pd
import re



# loading somepartner data
# old format _contents:  Daily Reporting
# def insert_recent_somepartner_data(_contents='contentsname'):
#     print("insert_recent_somepartner_data START: _contents: ", _contents)
#     em = get_email_wrapper()

#     data = read_csv_from_bytes(next(em.get_msg_w_attachments(_contents)))
#     print("insert_recent_somepartner_data: ", data)
#     print("insert_recent_somepartner_data string: \n", data.to_string())
#     # data['date'] = pd.to_datetime(data['Time']).dt.date
#     data['date'] = pd.to_datetime(data['Time'], format='%Y-%m-%d %H:%M:%S').dt.date
#     data.rename(columns={'Impressions': 'imps', 'Gross Revenue': 'spend'}, inplace=True)

#     print("insert_recent_somepartner_data data dropna ", data.dropna())
#     return insert_dataframe(data.dropna(), 'somepartner', 'usd')

# loading somepartner data
def insert_recent_somepartner_data(_contents='somecontents'):
    print("insert_recent_somepartner_data START: _contents: ", _contents)
    em = get_email_wrapper()

    data = read_csv_from_bytes(next(em.get_msg_w_attachments(sbj_contents)))
    print("insert_recent_somepartner_data: ", data)

    # Удаляем строки с 'Overall'
    data = data[data['Time'] != 'Overall']

    # Преобразуем столбец Time в datetime
    data['date'] = pd.to_datetime(data['Time'], format='%Y-%m-%d %H:%M:%S').dt.date

    # Убираем символ доллара и преобразуем столбец spend в числовой формат
    data['spend'] = data['Gross Revenue'].replace('[\$,]', '', regex=True).astype(float)

    data.rename(columns={'Impressions': 'imps'}, inplace=True)
    print("insert_recent_somepartner_data data dropna ", data.dropna())

    return insert_dataframe(data.dropna(), 'somepartner', 'usd')


def insert_monthly_somepartner_data():
    return insert_recent_somepartner_data('somecontents')


# # loading yetanothersomepartner data
# def insert_recent_yetanothersomepartner_data(_contents='somecontents'):
#     em = get_email_wrapper()

#     data = read_csv_from_bytes(next(em.get_msg_w_attachments(_contents))).dropna()
#     data['date'] = pd.to_datetime(data['Date Date']).dt.date
#     data['imps'] = data['Total Impression'].str.replace(",","").astype(int)
#     data['spend'] = data['Total Spend'].str[1:].str.replace(",","").astype(float)

#     return insert_dataframe(data.dropna(), 'yetanothersomepartner', 'usd')

# LOG / loading yetanothersomepartner data
def insert_recent_yetanothersomepartner_data(sbj_contents='somecontents'):
    print("Step 1: Initializing email wrapper")
    em = get_email_wrapper()

    print("Step 2: Getting email message with attachments")
    try:
        msg = next(em.get_msg_w_attachments(sbj_contents))
        print("Email with subject found and attachment extracted.")
    except Exception as e:
        print(f"Error getting email or attachments: {e}")
        return

    print("Step 3: Reading CSV from the attachment")
    try:
        data = read_csv_from_bytes(msg).dropna()
        print("CSV data read successfully.")
    except Exception as e:
        print(f"Error reading CSV from bytes: {e}")
        return

    print("Step 4: Converting 'Date Date' to datetime format")
    try:
        data['date'] = pd.to_datetime(data['Date Date']).dt.date
        print(f"Dates processed successfully. Sample: {data['date'].head()}")
    except Exception as e:
        print(f"Error processing dates: {e}")
        return

    print("Step 5: Cleaning 'Total Impression' column")
    try:
        data['imps'] = data['Total Impression'].str.replace(",", "").astype(int)
        print(f"Impressions processed successfully. Sample: {data['imps'].head()}")
    except Exception as e:
        print(f"Error processing impressions: {e}")
        return

    print("Step 6: Cleaning 'Total Spend' column")
    try:
        data['spend'] = data['Total Spend'].str[1:].str.replace(",", "").astype(float)
        print(f"Spend processed successfully. Sample: {data['spend'].head()}")
    except Exception as e:
        print(f"Error processing spend: {e}")
        return

    print("Step 7: Inserting data into the database")
    try:
        result = insert_dataframe(data.dropna(), 'yetanothersomepartner', 'usd')
        print("Data inserted successfully into the database.")
        return result
    except Exception as e:
        print(f"Error inserting data into the database: {e}")
        return


# # loading yetyetanothersomepartner data
# def insert_recent_yetyetanothersomepartner_data(_contents='somecontents'):
#     print("YETYETANOTHERSOMEPARTNER : Start insert_recent_yetyetanothersomepartner_data loading email data")
#     em = get_email_wrapper()

#     # Чтение данных из файла и вывод первых нескольких строк
#     data = read_excel_from_bytes(next(em.get_msg_w_attachments(sbj_contents)), header=1)
#     print("EMAIL YETANOTHERSOMEPARTNER : Initial data loaded from Excel:", data.head())  # Печатаем первые 5 строк

#     # Преобразование даты и вывод информации о колонке с датой
#     data['date'] = pd.to_datetime(data['Publisher Report (EDSP) Event Date']).dt.date
#     print("EMAIL YETYETANOTHERSOMEPARTNER : Data after date conversion:", data[['Publisher Report (EDSP) Event Date', 'date']].head())

#     # Преобразование количества показов и затрат
#     data['imps'] = data['Advertiser Impression Count'].astype(int)
#     data['spend'] = data['Advertiser Ad Spend'].astype(float)
#     print("EMAIL YETYETANOTHERSOMEPARTNER : Data after imps and spend conversion:", data[['imps', 'spend']].head())

#     # Удаление строк с пропущенными значениями и вывод финального состояния данных
#     cleaned_data = data.dropna()
#     print("EMAIL YETYETANOTHERSOMEPARTNER : Cleaned data (after dropping NaN values):", cleaned_data.head())

#     # Вставка данных в базу данных
#     return insert_dataframe(cleaned_data, 'yetyetanothersomepartner', 'usd')


from io import BytesIO

def insert_recent_yetyetanothersomepartner_data(sbj_contents='somecontents3'):
    print("Start insert_recent_yetyetanothersomepartner_data loading email data")
    em = get_email_wrapper()

    column_names = ['Publisher Report Event Date', 'Advertiser Ad Spend', 'Advertiser Impression Count']
    try:
        attachment = next(em.get_msg_w_attachments(sbj_contents))

        if isinstance(attachment, BytesIO):
            data = pd.read_csv(attachment, header=None, names=column_names, skiprows=1)
        else:
            data = pd.read_csv(BytesIO(attachment), header=None, names=column_names, skiprows=1)

        print("Initial data loaded from CSV:", data.head())  # Печатаем первые 5 строк
        print("data columns", data.columns)
    except Exception as e:
        print(f"Error while loading email data: {e}")
        return

    data['date'] = pd.to_datetime(data['не-помню-уже-что-здесь-должно-быть']).dt.date
    print("Data after date conversion:", data[['и-здесь-...-вам-надо-понять']].head())

    # data['Advertiser Impression Count'] = data['Advertiser Impression Count'].str.replace(',', '')
    # data['Advertiser Ad Spend'] = data['Advertiser Ad Spend'].str.replace(',', '')

    # data['imps'] = data['Advertiser Impression Count'].astype(int)
    # data['spend'] = data['Advertiser Ad Spend'].astype(float)
    # print("Data after imps and spend conversion:", data[['imps', 'spend']].head())

    data['Advertiser Ad Spend'] = data['Advertiser Ad Spend'].str.replace(r'[$,]', '', regex=True)
    data['Advertiser Impression Count'] = data['Advertiser Impression Count'].str.replace(',', '')

    data['imps'] = data['Advertiser Impression Count'].astype(int)
    data['spend'] = data['Advertiser Ad Spend'].astype(float)
    print("Data after imps and spend conversion:", data[['imps', 'spend']].head())

    cleaned_data = data.dropna()
    print("Cleaned data (after dropping NaN values):", cleaned_data.head())

    return insert_dataframe(cleaned_data, 'yetyetanothersomepartner', 'usd')

def insert_recent_partner_s_data(_contents='общий день',date=None):
    em = get_email_wrapper()

    for cnt in em.get_msg_w_attachments(sbj_contents):
        data = read_excel_from_bytes(cnt, header=0)

        print(data)
        data_partner = {}
        for _, rec in data.iterrows():
            if rec['День'] == '---':
                data_partner['dsp_id'] = 31
                data_partner['dsp_reqs'] = int(rec['Кол-во запросов'])
                data_partner['dsp_resp'] = int(rec['Кол-во загрузок'])
                data_partner['imps'] = int(rec['Кол-во показов'])
                data_partner['clicks'] = int(rec['Кол-во кликов'])
                data_partner['view100'] = int(rec['Досмотры 100%'])
                data_partner['currency'] = 'rub'
                break
            #elif re.match(r'\d\d\d\d-\d\d-\d\d', rec['День']):
            else:
                if rec['День'] == date:
                    data_partner['date'] = pd.to_datetime(rec['День']).date()
                else:
                    print(rec['День'])
                    break

        if 'date' in data_partner:
            return data_partner

