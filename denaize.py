import io
import zipfile

import numpy as np
import pandas as pd
from fastapi import Response
from statsmodels.tsa.seasonal import seasonal_decompose

from logger_conf import logger


def _create_df_well(df, file_name):
    """
    :param df: Исходный DataFrame
    :param file_name: Имя исходного файла
    :return: DataFrame с индексом типа DatetimeIndex
    """
    df_well = df[['Дата замера', 'Обводненность(объемная)', 'Скважина']]
    if file_name.endswith('csv'):
        df_well['Дата замера'] = pd.to_datetime(df['Дата замера'], utc=True)

    df_well.set_index('Дата замера', inplace=True)
    df_well['Month'] = df_well.index.month
    df_well['Y-m'] = df_well.index.year
    df_well['Y-m'] = df_well['Y-m'].astype(str) + '-' + \
                     df_well['Month'].astype(str)

    df_well.index = df_well.index.tz_localize(None)

    return df_well


def _get_dataframe_from_bytes(
        contents,
        file_name,
        sheet_name=None,
        n_rows=None,
        list_wells=None,
):
    """
    DataFrame from xlsx or xls or csv
    :param contents: byte строка файла
    :param list_wells: список скважин
    :param str file_name: file for dataframe
    :param str sheet_name: for Excel file
    :param int n_rows: for cut
    :return: DataFrame
    """
    if file_name.endswith('.xlsx') or file_name.endswith('.xls'):
        df = pd.ExcelFile(contents).parse(
            sheet_name=sheet_name,
            nrows=n_rows,
        )
    elif file_name.endswith('.csv'):
        df = pd.read_csv(
            io.BytesIO(contents),
            nrows=n_rows
        )
    else:
        e = f'Неверный формат файла {file_name}'
        logger.exception(e)
        return e

    df['Скважина'] = df['Скважина'].astype(str)

    if len(list_wells[0]) > 0:
        df = df[df['Скважина'].isin(list_wells)]

    return df


def _decompose_wells(df_well):
    """
    Добавляет колонку trend_sesonialclean
    :param df_well: DataFrame, в котором индексы (Y-m-d)
    """
    try:
        series = df_well[['Обводненность(объемная)']]
        result = seasonal_decompose(series, model='additive')
        # тренд применяется к скважине
        df_well['trend_sesonialclean'] = np.array(result.trend)
        return df_well
    except Exception as e:
        logger.exception(e)


def create_files(
        contents,
        file_name,
        sheet_name=None,
        n_rows=None,
        list_wells=None,

):
    """
    Create 2 new Excel file
    :param contents: Файл считанный в байты
    :param list_wells: Список клапанов
    :param int n_rows: количество считываемых строк
    :param str sheet_name: имя страницы
    :param str file_name: полное имя полученного файла с форматом
    """
    # Get DataFrame
    df = _get_dataframe_from_bytes(
        contents,
        file_name,
        sheet_name,
        n_rows,
        list_wells,
    )
    if isinstance(df, str):
        return df

    # create df_well
    df_well = _create_df_well(df, file_name)

    # Для каждой скважины отдельно просчитываем seasonal_decompose
    res_df_well = df_well.groupby('Скважина').apply(_decompose_wells)

    # Найти средние значения по дате
    by_average = res_df_well.groupby('Y-m').apply(
        lambda x: x['trend_sesonialclean'].mean())

    # to Byte Excel
    res = [_to_bytes_excel(x) for x in [by_average, res_df_well]]

    return res


def _to_bytes_excel(df):
    """
    Сделает байт-строку Excel файла из DataFrame
    :param df: DataFrame
    :return: bytes
    """
    with io.BytesIO() as buffer:
        with pd.ExcelWriter(buffer) as writer:
            df.to_excel(writer)
        excel_bytes = buffer.getvalue()
        return excel_bytes


def response_zip(files, filenames):
    """
    Сделает байт-строку zip-файла, внутри которого байт строки других файлов
    :param list files:
    :param list filenames:
    :return: Response
    """
    try:
        zip_filename = 'archive.zip'

        s = io.BytesIO()
        with zipfile.ZipFile(s, 'w') as zf:
            for file, filename in zip(files, filenames):
                zf.writestr(filename, file)

        resp = Response(
            s.getvalue(), media_type='application/x-zip-compressed',
            headers={
                'Content-Disposition': f'attachment;filename={zip_filename}'
            })
        return resp
    except Exception as e:
        logger.exception(e)
