from cmath import e
import sys
import time
from urllib import response
from importlib_metadata import metadata
from matplotlib.image import thumbnail
import ray
import requests
import random
import pandas as pd
import argparse
import os
import functools
from ray.autoscaler.sdk import request_resources

from dataclasses import dataclass, fields

""" Run this script locally to execute a Ray program on your Ray cluster on
Kubernetes.

Before running this script, you must port-forward from the local host to
the relevant Kubernetes head service e.g.
kubectl -n ray port-forward service/example-cluster-ray-head 10001:10001.

Set the constant LOCAL_PORT below to the local port being forwarded.
"""
LOCAL_PORT = 10001

def wait_for_nodes(expected):
    # Wait for all nodes to join the cluster.
    while True:
        resources = ray.cluster_resources()
        node_keys = [key for key in resources if "node" in key]
        num_nodes = sum(resources[node_key] for node_key in node_keys)
        if num_nodes < expected:
            print("{} nodes have joined so far, waiting for {} more.".format(
                num_nodes, expected - num_nodes))
            sys.stdout.flush()
            time.sleep(1)
        else:
            break

sys.path.append('/home/nando/PhD/Ray/ray/ray-apps')
from ResourceAllocator.resource_allocator import rManager, resourceWrapperStress


IMAGE_NAME = "image_name"
EXTRACTED_METADATA = "extracted_metadata"
THUMBNAIL_NAME = "thumbnail"
from PIL import Image, ExifTags 

@resourceWrapperStress
def extractImgMetadata(imgURL):
    # print("Extracting img metadata")

    imgPath = "test.jpg"
    r = requests.get(imgURL, allow_redirects=True)
    open(imgPath, 'wb').write(r.content)
    
    img = Image.open(imgPath)
    # File is only required to open image. Delete after used
    removeFile(imgPath)
    img_exif = img.getexif()
    img_exif_w_tags = {}

    if img_exif is None:
        print("It seems the image has no exif data.")
        img_exif = {}
    else:
        for key, val in img_exif.items():
            if key in ExifTags.TAGS:
                # print(f'{ExifTags.TAGS[key]}:{val}, {key}')
                img_exif_w_tags[ExifTags.TAGS[key]] = val

    response = {}
    response[IMAGE_NAME] = imgPath
    response[EXTRACTED_METADATA] = img_exif_w_tags

    return response

@resourceWrapperStress
def transformMetadata(args):
    # print("Transforming metadata")
    response = {}
    response[IMAGE_NAME] = args[IMAGE_NAME]

    extracted_metadata = args[EXTRACTED_METADATA]
    transformed_metadata = {}
    if ("DateTimeOriginal" in extracted_metadata):
        transformed_metadata["creationTime"] = extracted_metadata["DateTimeOriginal"]

    if ({"GPSLatitude", "GPSLatitudeRef", "GPSLongitude", "GPSLongitudeRef"} <= set(extracted_metadata)):
            latitude = parseCoordinate(extracted_metadata["GPSLatitude"], extracted_metadata["GPSLongitudeRef"])
            longitude = parseCoordinate(extracted_metadata["GPSLongitude"], extracted_metadata["GPSLongitudeRef"])
            geo = {}
            geo["latitude"] = latitude
            geo["longitude"] = longitude
            transformed_metadata["geo"] = geo

    if ("Make" in extracted_metadata):
        transformed_metadata["exifMake"] = extracted_metadata["Make"]

    if ("Model" in extracted_metadata):
        transformed_metadata["exifModel"] = extracted_metadata["Model"]

    dimensions = {}
    dimensions["width"] = int(extracted_metadata["ImageWidth"])
    dimensions["height"] = int(extracted_metadata["ImageLength"])
    transformed_metadata["dimensions"] = dimensions

    # These two exif tags were not used originally
    # Instead the filesize and format tags were used. 
    # I'm using different tags because the original ones were not present in the test image
    transformed_metadata["bitsPerSample"] = extracted_metadata["BitsPerSample"]
    transformed_metadata["software"] = extracted_metadata["Software"]

    response[EXTRACTED_METADATA] = transformed_metadata
    return response

def parseCoordinate(coordinate, coordinateDirection):
    degreeArray = coordinate.split(",")[0].trim().split("/")
    minuteArray = coordinate.split(",")[1].trim().split("/")
    secondArray = coordinate.split(",")[2].trim().split("/")

    ret = {}
    ret["D"] = int(degreeArray[0]) / int(degreeArray[1])
    ret["M"] = int(minuteArray[0]) / int(minuteArray[1])
    ret["S"] = int(secondArray[0]) / int(secondArray[1])
    ret["Direction"] = coordinateDirection
    return ret

@resourceWrapperStress
def handler(args):
    # print("Logging data")
    return args

@resourceWrapperStress(num_returns=2)
def thumbnail(args, imgURL, max_size=(250, 250)):
    # print("Creating thumbnail")

    response = args

    imageName = args[IMAGE_NAME]
    size = args[EXTRACTED_METADATA]["dimensions"]
    width = size["width"]
    height = size["height"]

    scalingFactor = min(max_size[0]/width, max_size[1]/height)
    width = int(width * scalingFactor)
    height = int(height * scalingFactor)

    thumbnailName = "thumbnail-" + imageName
    r = requests.get(imgURL, allow_redirects=True)
    open(thumbnailName, 'wb').write(r.content)
    
    image = Image.open(thumbnailName)
    # File is only required to open the image
    removeFile(thumbnailName)
    image.thumbnail(size=(width, height))

    response[EXTRACTED_METADATA][THUMBNAIL_NAME] = thumbnailName

    return response, image

@resourceWrapperStress
def returnMetadata(args):
    # print("Returning metadata")
    return args[EXTRACTED_METADATA]

# @ray.remote
def createThumbnail(imgPath, max_size=(100, 100)):
    # print(f"Creating thumbnail for image at ={imgPath}")
    start = time.time()

    imgMRef = extractImgMetadata.remote(imgPath)
    tfRef = transformMetadata.remote(imgMRef)
    hRef = handler.remote(tfRef)
    tRef, imgRef = thumbnail.remote(hRef, imgPath, max_size)
    rMRef = returnMetadata.remote(tRef)
    image = ray.get(imgRef)
    metadata = ray.get(rMRef)
    
    # print(f"Transformed metadata.")
    # print(f"ExtractedMetadata: {metadata=}")
    # print(f"Saving image")
    image.save(metadata[THUMBNAIL_NAME])

    execTime = time.time() - start
    print(f"ExecTime = {round(execTime, 2)}s")

import os
def removeFile(filename):
    if os.path.exists(filename):
        os.remove(filename)
    else:
        print("The file does not exist!")
        exit(1)

def main(imgPath):
    # transformedMetadata = ray.get(createThumbnail.remote(imgPath))
    createThumbnail(imgPath)

    print("Thumbnailer finished")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='PageFetcher on Ray')
    parser.add_argument('--exp_name', type=str, help='Name of experiment to prepend to output files', required=False)
    parser.add_argument('--imgPath', type=str, help='Path to image', default="https://raw.githubusercontent.com/SJTU-IPADS/ServerlessBench/master/Testcase4-Application-breakdown/image-process/assets/test.jpg")
    # parser.add_argument('--imgPath', type=str, help='Path to image', default="test.jpg")
    parser.add_argument('--desired_SLO', type=int, help='SLO in ms',  default=0)

    args = parser.parse_args()
    exp_name = args.exp_name
    imgPath = args.imgPath
    desired_SLO = args.desired_SLO

    # runtime_env = {"working_dir": "./assets", "py_modules": ["../ResourceAllocator"], "pip": ["pillow", "bayesian-optimization"]}
    # runtime_env = {"py_modules": ["../ResourceAllocator"], "pip": ["pillow", "requests", "bayesian-optimization"]}
    # runtime_env = {"py_modules": ["../ResourceAllocator"]}
    runtime_env = {}
    ray.init(f"ray://127.0.0.1:{LOCAL_PORT}", runtime_env=runtime_env)
    wait_for_nodes(2)

    rManager.optimize(lambda: main(imgPath), SLO=desired_SLO)
    # main(imgPath)

    # import threading
    # stop_event = threading.Event()
    # stressCPU(1, stop_event)


    sys.stdout.flush()
    ray.shutdown()
