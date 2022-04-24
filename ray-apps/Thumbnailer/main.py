from cmath import e
import sys
import time
from urllib import response
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
from ResourceAllocator.resource_allocator import rManager, resourceWrapper


IMAGE_NAME = "image_name"
EXTRACTED_METADATA = "extracted_metadata"
from PIL import Image, ExifTags 
@resourceWrapper
@ray.remote
def extractImgMetadata(imgURL):
    print("Extracting img metadata")

    imgPath = "thumbnail_pre.jpg"
    r = requests.get(imgURL, allow_redirects=True)
    open(imgPath, 'wb').write(r.content)
    
    img = Image.open(imgPath)
    img_exif = img.getexif()
    img_exif_w_tags = {}

    if img_exif is None:
        print("It seems the image has no exif data.")
        img_exif = {}
    else:
        for key, val in img_exif.items():
            if key in ExifTags.TAGS:
                print(f'{ExifTags.TAGS[key]}:{val}, {key}')
                img_exif_w_tags[ExifTags.TAGS[key]] = val

    response = {}
    response[IMAGE_NAME] = imgPath
    response[EXTRACTED_METADATA] = img_exif_w_tags

    return response


@resourceWrapper
@ray.remote
def transformMetadata(args):
    response = {}
    response[IMAGE_NAME] = args[IMAGE_NAME]

    extracted_metadata = args[EXTRACTED_METADATA]
    print("Transforming metadata")
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
    transformed_metadata["bitsPerSample:"] = extracted_metadata["BitsPerSample:"]
    transformed_metadata["software"] = extracted_metadata["Software"]

    response[EXTRACTED_METADATA] = transformMetadata
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

@resourceWrapper
@ray.remote
def handler(metadata):
    print("Logging data")
    return metadata

@resourceWrapper
@ray.remote(num_returns=2)
def thumbnail(metadata, imgURL, max_size=(100, 100)):
    print("Creating thumbnail")
    
    imgPath = "test.jpg"
    r = requests.get(imgURL, allow_redirects=True)
    open(imgPath, 'wb').write(r.content)
    
    image = Image.open(imgPath)
    image.thumbnail(max_size)

    return metadata, image

@resourceWrapper
@ray.remote
def returnMetadata(metadata, image):
    print("Returning metadata")
    return image

# @ray.remote
def createThumbnail(imgPath, max_size=(100, 100)):
    print(f"Creating thumbnail for image at ={imgPath}")
    start = time.time()

    imgMRef = extractImgMetadata.remote(imgPath)
    tfRef = transformMetadata.remote(imgMRef)
    hRef = handler.remote(tfRef)
    mRef, imgRef = thumbnail.remote(hRef, imgPath, max_size)
    rMRef = returnMetadata.remote(mRef, imgRef)
    image = ray.get(rMRef)
    
    image.save('thumb.png')
    print(f"Transformed metadata.")

    execTime = time.time() - start
    print(f"ExecTime = {round(execTime, 2)}s")

def main(imgPath):
    # runtime_env = {"working_dir": "./assets", "py_modules": ["../ResourceAllocator"], "pip": ["pillow", "bayesian-optimization"]}
    # runtime_env = {"py_modules": ["../ResourceAllocator"], "pip": ["pillow", "requests", "bayesian-optimization"]}
    runtime_env = {}
    ray.init(f"ray://127.0.0.1:{LOCAL_PORT}", runtime_env=runtime_env)
    wait_for_nodes(2)
    
    # transformedMetadata = ray.get(createThumbnail.remote(imgPath))
    createThumbnail(imgPath)

    print(f"Transformed img")

    sys.stdout.flush()
    ray.shutdown()
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

    # rManager.optimize(lambda: main(imgPath), SLO=desired_SLO)
    main(imgPath)
