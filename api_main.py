from fastapi import FastAPI, Query, UploadFile, File
import datetime as dt
from io import StringIO
from fastapi.responses import StreamingResponse
from fastapi import status
import pandas_sqlite_processing as ps_proc
import aws_processing as aws_proc
import analytics

"""
	Project uses FastAPI library, ideally running in uvicorn
"""

app = FastAPI()


@app.get("/get-weekly")
async def weekly(source: str = 'lite'
                 , lat1: float = Query(None, description="First latitude of bounding box")
                 , long1: float = Query(None, description="First longitute of bounding box")
                 , lat2: float = Query(None, description="Second latitude of bounding box")
                 , long2: float = Query(None, description="Second longitute of bounding box")
                 , region: float = Query(None, description="Region")):
    # assumption : lat/long 1 is top left corner and lat/long 2 is bottom right corner

    # getting a list of coordinates and checking if all values are present
    coords = [lat1, lat2, long1, long2]
    is_complete_box = all([i != None for i in coords])

    # routing to appropriate function, either sqlite or aws variant
    if source == 'lite':
        if is_complete_box == True and region is None:
            return "No arguments provided. Set lat1, long1, lat1, lat2 or region"
        elif is_complete_box == False and region != None:
            return analytics.get_averages_region_lite(region)
        elif is_complete_box == True and region is None:
            return analytics.get_averages_box_lite(lat1, long1, lat2, long2)
        elif is_complete_box == True and region != None:
            return analytics.get_averages_region_box_lite(lat1, long1, lat2, long2, region)
    else:
        if is_complete_box == True and region is None:
            return "No arguments provided. Set lat1, long1, lat1, lat2 or region"
        elif is_complete_box == False and region != None:
            return analytics.get_averages_region_aws(region)
        elif is_complete_box == True and region is None:
            return analytics.get_averages_box_aws(lat1, long1, lat2, long2)
        elif is_complete_box == True and region != None:
            return analytics.get_averages_region_box_aws(lat1, long1, lat2, long2, region)
    return 1


@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    # requirement being that the file is .csv

    # reading the file, storing in memory and passing to the transformation layer
    content = await file.read()
    decoded = content.decode()
    buffer = StringIO(decoded)
    response = StreamingResponse(content = ps_proc.start(buffer, 'lite'), status_code=status.HTTP_200_OK, media_type="text/html")
    return response


@app.post('/aws-upload-trigger')
async def aws_upload_trigger(filepath: str):
    # accepts s3 path
    file = aws_proc.get_s3_file()
    return ps_proc.start(file, 'aws')



