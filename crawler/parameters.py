from typing import List, Tuple
from datetime import date
import os

class CrawlEngineParameters:

    def __init__(self, 
                 census_year: int, 
                 isilon_root: str,
                 scan_folder: Tuple[int,int], 
                 reel_batch_size: int = None,
                 image_batch_size: int = None, 
                 exclusion_filters = [],
                 extension_filters = ['.jpg']) -> None:

        if not exclusion_filters:
            self.__exclusion_filters = List[str] 
        else:
            self.__exclusion_filters = exclusion_filters

        self.__census_year = census_year
        self.__scan_folder = scan_folder
        self.__exclusion_filters = exclusion_filters
        self.__isilon_root = isilon_root
        self.__reel_batch_size = reel_batch_size
        self.__image_batch_size = image_batch_size
        self.__extension_filters = extension_filters

    @property
    def census_year(self) -> int:
        return self.__census_year

    @property
    def scan_folder(self) -> Tuple[int,int]:
        return self.__scan_folder

    @property
    def isilon_root(self) -> str:
        return self.__isilon_root

    @property
    def reel_batch_size(self) -> int:
        return self.__reel_batch_size
    
    @property
    def image_batch_size(self) -> int:
        return self.__image_batch_size

    @property
    def exclusion_filters(self) -> List[str]:
        return self.__exclusion_filters

    @property
    def extension_filters(self) -> List[str]:
        return self.__extension_filters

    @property
    def normalized_folder(self) -> str:
        return (date(self.scan_folder[1], self.scan_folder[0], 1)
                                   .strftime("%m-%b-%y"))

    @property
    def scan_month_name(self) -> str:
        return (date(self.scan_folder[1], self.scan_folder[0], 1)
                                   .strftime("%b"))

    @property
    def scan_month(self) -> int:
        return self.__scan_folder[0]

    @property
    def scan_year(self) -> int:
        return self.__scan_folder[1]

    @property
    def scan_dir(self) -> str:
        return os.path.join(self.isilon_root, str(self.census_year),
                                      str(self.scan_year),
                                      self.normalized_folder)


    def __str__(self):

        output = f"Census Year: {self.census_year}\n"
        output = output + f"Isilon Root: {self.isilon_root}\n"
        output = output + f"Scan Folder: {self.scan_folder}\n"
        output = output + f"Exclusion Filters: {self.exclusion_filters}\n"
        output = output + f"Extension Filters: {self.extension_filters}\n"
        output = output + f"Reel Batch Size: {self.reel_batch_size}\n"
        output = output + f"Image Batch Size: {self.image_batch_size}\n"

        return output


class RunRecrawlParameters:

    def __init__(self,
                    census_year: int, 
                    scan_month: int,
                    scan_year: int,
                    persist_data: bool = True,
                 ) -> None:
        self.__persist_data = persist_data
        self.__census_year = census_year
        self.__scan_month = scan_month
        self.__scan_year = scan_year

    @property
    def census_year(self) -> int:
        return self.__census_year 

    @property
    def scan_year(self) -> int:
        return self.__scan_year 

    @property
    def scan_month(self) -> int:
        return self.__scan_month

    @property
    def persist_data(self) -> bool:
        return self.__persist_data 


    def __str__(self) -> str:

        output = f'Current Crawl Parameters:\n'
        output = output + f'Persist Data (Live-Run): {self.persist_data}\n'
        output = output + f'Census Year: {self.census_year}\n'
        output = output + f'Scan Year: {self.scan_year}\n'
        output = output + f'Scan Month: {self.scan_month}\n'

        return str(output)


class RunCrawlParameters:

    def __init__(self, 
                    census_year: int, 
                    scan_month: int,
                    scan_year: int
        ) -> None:
        self.__census_year = census_year
        self.__scan_month = scan_month
        self.__scan_year = scan_year
        self.__persist_data = True
        self.__reel_batch_limit = None
        self.__image_batch_limit = None    
    
    @property
    def census_year(self) -> int:
        return self.__census_year 

    @property
    def scan_year(self) -> int:
        return self.__scan_year 

    @property
    def scan_month(self) -> int:
        return self.__scan_month

    @property
    def persist_data(self) -> bool:
        return self.__persist_data 
    
    @persist_data.setter
    def persist_data(self, value):
        self.__persist_data = value 

    @property
    def reel_batch_limit(self) -> int:
        return self.__reel_batch_limit

    @reel_batch_limit.setter
    def reel_batch_limit(self, value) -> int:
        self.__reel_batch_limit = value

    @property
    def image_batch_limit(self) -> int:
        return self.__image_batch_limit

    @image_batch_limit.setter
    def image_batch_limit(self, value) -> int:
        self.__image_batch_limit = value

    def __str__(self) -> str:
        
        output = f'Current Crawl Parameters:\n'
        output = output + f'Persist Data (Live-Run): {self.persist_data}\n'
        output = output + f'Census Year: {self.census_year}\n'
        output = output + f'Scan Year: {self.scan_year}\n'
        output = output + f'Scan Month: {self.scan_month}\n'
        output = output + f'Reel Batch Limit: {self.reel_batch_limit}\n'
        output = output + f'Image Batch Limit: {self.image_batch_limit}\n'

        return str(output)

