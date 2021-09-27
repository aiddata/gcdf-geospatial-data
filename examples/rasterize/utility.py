
import shutil
from warnings import warn
import numpy as np
from affine import Affine
import rasterio
from rasterio import features
from rasterio.windows import Window


class Grid():
    """Manage grid of data (raster) produced while rasterizing geometries

    Creates empty raster on initialization using geometry properties (bounds, shape, pixel_size)

    Updates raster in place with data for indivudal rasterized geometries
        - By continually updating the raster with new data from only a single
        geometry at a time, we can avoid storing a very large array representing
        the full dataset in memory.
    """
    def __init__(self, geom, pixel_size, dtype='uint32', nodata_val=0):
        # input args
        self.pixel_size = None
        self.dtype = dtype
        self.nodata_val = nodata_val
        # class attributes to be defined during init
        self.shape = None
        self.bounds = None
        self.affine = None
        # initialize class
        self.set_pixel_size(pixel_size)
        self.init_grid(geom)
        # temporary raster path used during processing
        self.init_raster_path = "blank_raster.tif"
        self.init_raster()


    def set_pixel_size(self, value):
        """Set pixel size.

        Args:
            value (float): new pixel size (max value of 1, no min)

        Setter will validate pixel size and set attribute.
        Also calculates psi (pixel size inverse) and sets attribute.
        """
        try:
            value = float(value)
        except:
            raise Exception("pixel size given could not be converted to " +
                            "float: " + str(value))
        # check for valid pixel size
        # examples of valid pixel sizes:
        # 1.0, 0.5, 0.25, 0.2, 0.1, 0.05, 0.025, ...
        if (1/value) != int(1/value):
            raise Exception("invalid pixel size: "+str(value))
        self.pixel_size = value
        self.psi = 1/value


    def init_grid(self, geom):
        """Define bounds, affine, and shape based on geometry provided
        """
        (minx, miny, maxx, maxy) = geom.bounds
        (minx, miny, maxx, maxy) = (
            round(np.floor(minx * self.psi)) / self.psi,
            round(np.floor(miny * self.psi)) / self.psi,
            round(np.ceil(maxx * self.psi)) / self.psi,
            round(np.ceil(maxy * self.psi)) / self.psi
        )
        self.bounds = (minx, miny, maxx, maxy)
        self.affine = Affine(self.pixel_size, 0, minx,
                        0, -self.pixel_size, maxy)
        nrows = int(np.ceil( (maxy - miny) / self.pixel_size ))
        ncols = int(np.ceil( (maxx - minx) / self.pixel_size ))
        self.shape = (nrows, ncols)


    def init_raster(self):
        """Create raster for grid
        """
        self.meta = {
            'count': 1,
            'crs': {'init': 'epsg:4326'},
            'dtype': self.dtype,
            'transform': self.affine,
            'driver': 'GTiff',
            'height': self.shape[0],
            'width': self.shape[1],
            'nodata': self.nodata_val,
            'compress': 'lzw',
            'tiled': True,
            'blockxsize': 512,
            'blockysize': 512
        }
        self.dst = rasterio.open(self.init_raster_path, "w+", **self.meta)


    def update(self, bounds, data):
        """Update grid raster based on bounds and data provided
        """
        ileft = int(round(np.floor((bounds[0] - self.bounds[0]) * self.psi) / (self.pixel_size * self.psi)))
        itop = int(round(np.floor((self.bounds[3] - bounds[3]) * self.psi) / (self.pixel_size * self.psi)))
        iright = ileft + data.shape[1]
        ibottom = itop + data.shape[0]
        try:
            update_window = Window(ileft, itop, data.shape[1], data.shape[0])
            existing = self.dst.read(1, window=update_window)
            self.dst.write(existing+data, window=update_window, indexes=1)

        except:
            print("#####")
            print("raster: shape ({}) bounds ({})".format(self.shape, self.bounds))
            print("data: shape ({}) bounds ({})".format(data.shape, bounds))
            print("data: left,right,top,bot ({})".format((ileft, iright, itop, ibottom)))
            print("#####")
            raise


    def save_geotiff(self, path):
        """Close raster and move to final destination
        """
        self.dst.close()
        shutil.move(self.init_raster_path, path)



# https://stackoverflow.com/questions/8090229/
#   resize-with-averaging-or-rebin-a-numpy-2d-array/8090605#8090605
def rebin_sum(a, shape, dtype):
    sh = shape[0], a.shape[0]//shape[0], shape[1], a.shape[1]//shape[1]
    return a.reshape(sh).sum(-1, dtype=dtype).sum(1, dtype=dtype)


def rasterize_geom(geom, pixel_size, scale=1, dtype='uint32'):
    """Rasterize geometry

    Args:
        geom: shapely geometry to be rasterized
        pixel_size (float): pixel size in decimal degrees
        scale (int): Factor used to determine subgrid size relative to output grid size.
                    Must be divisible by 2 and 5.
                    `sub grid res = output grid res / scale`
        dtype (str): rasterio data type of output raster

    Returns:
        rasterized array (np array), bounds (tuple)
    """
    if not hasattr(geom, 'geom_type'):
        raise Exception("invalid geom (has no geom_type)")

    try:
        fscale = float(scale)
        if fscale < 1:
            raise ValueError("scale must be >=1")
        scale = int(scale)
        if float(scale) != fscale:
            warn(f"scale float ({fscale}) converted to int ({scale})")
    except:
        raise TypeError(f"invalid type for scale ({type(scale)})")

    if scale == 1:
        sub_pixel_size = pixel_size
    elif scale % 2 != 0 and scale % 5 != 0:
        raise ValueError(f"scale must be divisible by 2 or 5 to avoid aggregation issues")
    else:
        sub_pixel_size = pixel_size / scale

    psi = 1 / pixel_size
    (minx, miny, maxx, maxy) =  geom.bounds
    (minx, miny, maxx, maxy) = (
        round(np.floor(minx * psi)) / psi,
        round(np.floor(miny * psi)) / psi,
        round(np.ceil(maxx * psi)) / psi,
        round(np.ceil(maxy * psi)) / psi)

    tmp_shape = [int((maxy - miny) / pixel_size),
             int((maxx - minx) / pixel_size)]

    for i in range(len(tmp_shape)):
        if tmp_shape[i] < 1:
            tmp_shape[i] = 1

    shape = tuple(tmp_shape)

    sub_shape = (shape[0] * scale, shape[1] * scale)

    sub_affine = Affine(sub_pixel_size, 0, minx,
                    0, -sub_pixel_size, maxy)

    rasterized = features.rasterize(
        [(geom, 1)],
        out_shape=sub_shape,
        transform=sub_affine,
        fill=0,
        all_touched=True
    )

    affine = Affine(pixel_size, 0, minx,
                    0, -pixel_size, maxy)

    if scale != 1:
        min_dtype = np.min_scalar_type(scale**2)
        rebin_dtype = dtype if np.iinfo(min_dtype).max < np.iinfo(dtype).max else min_dtype
        rv_array = rebin_sum(rasterized, shape, dtype=rebin_dtype)
    else:
        rv_array = rasterized

    if rv_array.sum() == 0:
        warn("No data was rasterized. Setting topleft cell to 1")
        rv_array[0][0] = 1

    return rv_array, (minx, miny, maxx, maxy)

