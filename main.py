import uvicorn
from fastapi import FastAPI, UploadFile
from fastapi.middleware.cors import CORSMiddleware

from denaize import response_zip, create_files

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/")
async def upload_excel_file(
        file: UploadFile,
        daily_aggregated_filename: str = "daily_aggregated",
        by_average_filename: str = "by_average",
        sheet_name: str = 'Шахматка_Часть 1',
        n_rows: int = None,
        list_wells: list = None,
):
    try:
        # names
        daily_aggregated_filename += ".xlsx"
        by_average_filename += ".xlsx"

        # file to bytes
        file_name = file.filename
        contents = file.file.read()

        # create 2 new file
        res = create_files(
            contents,
            file_name,
            sheet_name,
            n_rows,
            list_wells,
        )
        if isinstance(res, str):
            return res

    except Exception as e:
        print(e)
        return e
    return response_zip(
        files=res,
        filenames=[
            daily_aggregated_filename,
            by_average_filename
        ]
    )


# http://localhost:8000/docs

if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=8000)
