import shutil

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
async def upload_file(
        file: UploadFile,
        daily_aggregated_filename: str = "daily_aggregated",
        by_average_filename: str = "by_average",
        sheet_name: str = 'Шахматка_Часть 1',
        n_rows: int = 20,
        well_number: int = 101,
):
    try:
        # names
        daily_aggregated_filename += ".xlsx"
        by_average_filename += ".xlsx"

        # upload and save file
        with open(file.filename, 'wb') as f:
            file_name = file.filename
            shutil.copyfileobj(file.file, f)

        # create 2 new file
        create_files(
            file_name,
            daily_aggregated_filename,
            by_average_filename,
            sheet_name,
            n_rows,
            well_number,
        )

    except Exception as e:
        return e
    return response_zip(filenames=[
        daily_aggregated_filename, by_average_filename])

# http://localhost:8000/docs
