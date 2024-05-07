import os

from config import CrawlerSettings
from datetime import date
from logger import CrawlLogger
from typing import List, Tuple
from crawler.parameters import CrawlEngineParameters


"""
•	Scheduled process that’s run monthly after initial “catch-up” of previously scanned images.
•	Only adds newly discovered reels to database.
•	Avoid crawling directories provided in exclusion filters (ability to exclude test directories).
•	Batch configuration options to crawl/process images in “chunks.”
•	Smart enough to be date driven after initial “catch-up" crawl:
	o	If crawler doesn't need to backfill then we can make it smarter/faster by
                incorporating date driven design (after backfill) for example:
                  Crawler kicks off July 1st 2024, it knows to poll the 06-Jun-24 directory 
                   for census years [1960, 1970, 1980, 1990] (After initial backfill)
    #Done - Layer in error handling and populate result.errors_encountered
    #Done - Add in exclusion filter checking
    #Done - Beef up logging to support production runtime (rolling file appender)
    #Done - Add reel batch size to allow smaller runs (quicker runs on multiple test scenarios)
    - Add Image Batch Size to allow full test runs across multiple scan efforts
    - 

"""
class ReelCrawler:

    def __init__(self, 
                 prms: CrawlEngineParameters) -> None:

        self._prms = prms       

    def run_crawl(self):

        prms = self._prms
        reel_counter = 0
        image_counter = 0

        log = CrawlLogger()

        for scan_entry in os.scandir(prms.scan_dir):

            if scan_entry.name is not prms.scan_dir and scan_entry.name.find("frames") == -1:

                try:
                   
                    if len([filter for filter in prms.exclusion_filters if
                            filter == scan_entry.name]) == 0:

                        reel = {"census_year": prms.census_year,"scan_identifier": scan_entry.name, 
                                "scan_month": prms.scan_month,"scan_year": prms.scan_year, "scan_month_name": prms.scan_month_name,
                                "reel_filepath": scan_entry.path,"parsed_images": []}

                        result = {
                            "census_year" : prms.census_year,
                            "parsed_reel" : reel,
                            "folder_crawled" : (prms.scan_month, prms.scan_year)
                        }

                        # parse the images
                        images_path = os.path.join(scan_entry.path, "frames")

                        for image_entry in os.scandir(images_path):
                            if image_entry.is_file() and \
                                    len([x for x in prms.extension_filters if image_entry.name.endswith(x)]) > 0:
                                parsed_image = {"filename": str(image_entry.name), "filepath": image_entry.path,
                                        "filesize": image_entry.stat().st_size}

                                reel["parsed_images"].append(parsed_image)
                                image_counter = image_counter + 1

                                if prms.image_batch_size:
                                    if prms.image_batch_size == image_counter:
                                        image_counter = 0
                                        log.log_system_message(f"Stopping parsing of images for {reel['scan_identifier']} per image batch setting of: {prms.image_batch_size}")         
                                        break

                        reel_counter = reel_counter + 1
                        
                        #append the reel
                        result["reel"] = reel
                        
                        log.log_reel_parse(reel)
                        log.log_total_images_parsed(len(reel["parsed_images"]), reel["scan_identifier"])
                        yield result

                        if prms.reel_batch_size:
                            if prms.reel_batch_size == reel_counter:
                                log.log_system_message(f"Exiting crawler per batch setting of {prms.reel_batch_size}")
                                break

                except Exception as ex:
                    exmessage = f'Error in reel parsing operation at location: {scan_entry.path}'
                    exmessage += f'Error: {ex}'
                    log.log_error(exmessage)
