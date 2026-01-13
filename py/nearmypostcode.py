from collections.abc import Generator
from functools import cached_property
from datetime import datetime
import re
import math
import struct
from pathlib import Path

__all__ = ['nearmypostcode']

# Useful globals and type aliases
POSTCODE = re.compile('^[0-9A-Za-z ]+$')
Point = tuple[float, float]
Kilometers = float
Headers = tuple[int, int, int, int]

def uint32(b: bytes) -> Generator[int]:
    # unsigned int
    yield from (i[0] for i in struct.iter_unpack('I', b))

def uint16(b: bytes) -> Generator[int]:
    # unsigned short
    yield from (i[0] for i in struct.iter_unpack('H', b))

def uint8(b: bytes) -> Generator[int]:
    # unsigned char
    yield from (i[0] for i in struct.iter_unpack('B', b))

def int16(b: bytes) -> Generator[int]:
    # signed short
    yield from (i[0] for i in struct.iter_unpack('h', b))

def int8(b: bytes) -> Generator[int]:
    # signed char
    yield from (i[0] for i in struct.iter_unpack('b', b))

def float64(b: bytes) -> Generator[float]:
    # double
    yield from (i[0] for i in struct.iter_unpack('d', b))

class NearMyPostcode:
    __max_version__ = 2
    __magic__ = b'UKPP'
    __e_format__ = "Postcode format not recognised"
    __e_notfound__ = "Postcode not found"
    __e_data_version__ = "Data file format does not support this type of postcode"
    
    def __init__(self, datafile_url: str | Path) -> None:
        self.url = datafile_url
        self.deltapack = self._load_datafile(datafile_url)
        # Grab headers
        self.headers = self.deltapack[:16]
        # Discard headers from pack
        self.deltapack = self.deltapack[16:]
        self._validate_version()
        self._validate_format()
        
        # Get the extents of the postcode bounding box
        # Moved from `lookup_postcode` method to initialization
        self.minlong, self.maxlong, self.minlat, self.maxlat = float64(self.deltapack[0:32])
        
    def _load_datafile(self, datafile: str | Path) -> bytes:
        try:
            with open(datafile, 'rb') as fl:
                return fl.read()
        except Exception as e:
            raise ValueError(f'Failed to fetch postcode data file ({datafile})') from e
        
    def _validate_version(self) -> None:
        if self.version > self.__max_version__:
            raise ValueError(
                f'Postcode data file uses format version {self.version}. '
                f'This NMP version only supports data formats up to {self.__max_version__}. '
                'NMP needs to be updated.'
            )
    
    def _validate_format(self) -> None:
        if self.magic != self.__magic__:
            raise ValueError('Postcode data file is not using a known format')
    
    @cached_property
    def magic(self) -> bytes:
        return self.headers[0:4]
    @cached_property
    def version(self) -> int:
        return next(uint32(self.headers[4:8]))
    @cached_property
    def date(self) -> datetime:
        return datetime.fromtimestamp(sum(uint32(self.headers[8:16])))

    def pack_code(self, postcode: str) -> int:
        def encode_AZ(x: str) -> int:
            code = ord(x)
            if code >= ord('A') and code <= ord('Z'):
                return code - ord('A')
            raise ValueError(self.__e_format__)
            
        def encode_09(x: str) -> int:
            code = ord(x)
            if code >= ord('0') and code <= ord('9'):
                return code - ord('0')
            raise ValueError(self.__e_format__)
        
        def encode_AZ09(x: str) -> int:
            try:
                return encode_AZ(x)
            except ValueError:
                return encode_09(x)
                
        def encode_AZ09_space(x: str) -> int:
            if x == ' ':
                return ord(' ')
            return encode_AZ09(x)
        
        if len(postcode) == 4:
            _,_,c,d = postcode
            # Encode the rest
            c2 = 37*encode_AZ09_space(c)
            d2 = encode_AZ09_space(d)

            encoded = c2 + d2
            return encoded
        
        if len(postcode) == 7:
            _,_,c,d,e,f,g = postcode
            c2 = 26*26*10*37*encode_AZ09_space(c)
            d2 = 26*26*10*encode_AZ09_space(d)
            e2 = 26*26*encode_09(e)
            f2 = 26*encode_AZ(f)
            g2 = encode_AZ(g)
            encoded = c2 + d2 + e2 + f2 + g2
            return encoded
        raise ValueError(self.__e_format__)
    
    def format_postcode(self, postcode: str) -> str:
        if not POSTCODE.fullmatch(postcode):
            raise ValueError(self.__e_format__)
        numchars = len(postcode)
        
        if numchars > 7 or numchars < 2:
            raise ValueError(self.__e_format__)
        if numchars <=4:
            return f'{postcode:<4}'
        inward = postcode[numchars-3:]
        outward = postcode[:numchars-3]
        return f'{outward:<4}{inward}'
    
    # NOTE: There is something wrong with my ported logic that isn't properly 
    # indexing into the postcode tables
    def lookup_postcode(self, postcode: str) -> tuple[str, Point]:
        # Calculate the encoded value of this postcode
        postcode = self.format_postcode(postcode)
        lookup_outward_only = len(postcode) == 4
        if lookup_outward_only and self.version < 2:
            raise ValueError(self.__e_data_version__)
        c_code = self.pack_code(postcode) 
        c = [ # type: ignore (Not Used?)
            c_code & 0xff,
            (c_code >> 8) & 0xff,
            (c_code >> 16) & 0xff,
        ]
        
        # Use the two character prefix to find the offsets in the offset lookup table
        c1 = ord(postcode[0])
        c2 = ord(postcode[1])
        c2_i = c2 - ord('0') if c2 < ord('A') else 10 + c2 - ord('A')
        lut_index = ((c1 - ord('A'))*36)+c2_i
        lpos = (8*4) + (lut_index * 4)
        startpos, endpos = uint32(self.deltapack[lpos:lpos+8])
        
        # Scan the rest of the file from startpos to endpos looking for the postcode
        # (startpos is relative to the start of the postcode data, so calculate that offset first)
        
        # Skip extent(32), LUT(3744), and offset(4)
        datastart = (8*4) + (4*26*36) + 4
        pos = startpos + datastart
        last_code = 0
        last_lat = 0
        last_long = 0
        
        while pos < (endpos + datastart):
            is_outward_only = False
            # Get the format of this postcode entry (each field delta encoded or not)
            format = next(uint8(self.deltapack[pos:pos+1]))
            pos += 1
            pc_is_delta = (format & 0x80) > 0
            ll_is_delta = (format & 0x40) > 0
            # Calculate the postcode and lat/long by addition of the delta value or from absolute values
            # as specified in the format byte
            if pc_is_delta:
                # Postcode delta encoding is part of the format byte
                delta = format & 0x3f
                this_code = last_code + delta + 1
            else:
                special = format & 0x3f
                if special == 0x20:
                    is_outward_only = True
                    nc_a, nc_b, nc_c = uint8(self.deltapack[pos:pos+3])
                    pos += 3
                    this_code = (nc_c << 16) + (nc_b << 8) + nc_a
                else:
                    nc_a, nc_b, nc_c = uint8(self.deltapack[pos:pos+3])
                    pos += 3
                    this_code = (nc_c << 16) + (nc_b << 8) + nc_a
            
            if ll_is_delta:
                # lat/long is delta encoded as a pair of signed 8 bit numbers
                dlat, dlong = int8(self.deltapack[pos:pos+2])
                pos += 2
                long = last_long + dlong
                lat = last_lat + dlat
            else:
                # Absolute lat/long is a pair of 16 bit unsigned numbers
                lat, long = uint16(self.deltapack[pos:pos+4])
                pos += 4
            if is_outward_only == lookup_outward_only:
                if this_code == c_code:
                    # Calculate the real coordinates (the stored value is the fraction of the width or height of the bounding box)
                    lat2  = self.minlat +  (self.maxlat -self.minlat )*(lat/65535.0)
                    long2 = self.minlong + (self.maxlong-self.minlong)*(long/65535.0)
                    return (postcode, (long2,lat2))
            last_code = this_code
            last_lat = lat
            last_long = long

        raise ValueError(self.__e_notfound__)
        
    def distance_between(self, point_a: Point, point_b: Point) -> Kilometers:
        # https://en.wikipedia.org/wiki/Geographical_distance
        # This is an approximation of the geodesic distance b/w 
        # two points
        lat1, lon1 = point_a
        lat2, lon2 = point_b
        earth_radius_km = 6371
        d_lat = math.radians(lat2 - lat1)
        d_lon = math.radians(lon2 - lon1)
        a = (
            math.sin(d_lat / 2)**2 + 
            (
                math.cos(math.radians(lat1)) * 
                math.cos(math.radians(lat2)) * 
                math.sin(d_lon / 2)**2
            )
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return earth_radius_km * c
    
    def sort_by_distance(self, points: list[Point], point: Point) -> list[Point]:
        if len(point) != 2 and not isinstance(point, tuple | list): # type: ignore
            raise ValueError('point should be a pair of numbers: [lon, lat]')
        return sorted(points, key=lambda i: self.distance_between(i, point))
     
def nearmypostcode(datafile_url: str | Path, quiet: bool=False) -> NearMyPostcode:
    nmp = NearMyPostcode(datafile_url)
    if not quiet:
        print(
            'nearmypostcode: Loaded postcode pack. '
            f'Max supported file format version is {nmp.__max_version__}. '
            f'File format version is {nmp.version}. '
            f'Last updated {nmp.date.strftime('%a %b %d %Y')}'
        )
    return nmp

if __name__ == '__main__':
    # TODO: Use argparse to build a usable CLI
    import sys
    from pathlib import Path
    default_pack = Path(__file__).parent / 'postcodes.pack'
    nmp = nearmypostcode(default_pack, quiet=True)
    
    # Don't run main script in interactive mode, notify user of nmp instance
    if 'jupyter' in sys.argv[-1] and sys.argv[-1].endswith('json'):
        print(
            'Jupyter Mode active: \n'
            'Use `nmp.lookup_postcode(...)` \n'
            'to check a code interactively'
        )
    
    else:
        if len(sys.argv) == 1:
            print('Expected Postcode argument')
            sys.exit(1)
        try:
            print(nmp.lookup_postcode(sys.argv[1]))
            sys.exit(0)
        except ValueError as e:
            print(f'Failed to get postal code: {e}')
            sys.exit(1)