import os
import exifread
import rawpy

from datetime import datetime as dt
from functools import partial
from multiprocessing.pool import Pool

import astropy.io.fits as fits
import numpy as np

# params = [in_dir, out_dir, channel_subdirs, object]
params = [r"H:\Documents\Astro\Processing\UU Aur\Raw Data\Flats",
          r"H:\Documents\Astro\Processing\UU Aur\Separated Data\Flats", True, "UU Aur"]
# params = ["testdata", "testdata_out", True, "UU Aur"]


def process(file, params):
    raw = rawpy.imread(os.path.join(params[0], file))
    image = raw.postprocess(demosaic_algorithm=0, four_color_rgb=True, gamma=None, no_auto_bright=True, output_bps=16)
    tags = exifread.process_file(open(os.path.join(params[0], file), "rb"))
    # image = np.squeeze(image)
    images = np.dsplit(image, image.shape[-1])
    colors = ["R", "G", "B"]
    file = file.split(".", 1)[0]
    for i in range(3):
        images[i] = np.squeeze(images[i])
        hdu = fits.PrimaryHDU(images[i])
        if params[2]:
            path = os.path.join(params[1], colors[i], file + "_" + colors[i] + ".FITS")
        else:
            path = os.path.join(params[1], file + "_" + colors[i] + ".FITS")
        if not os.path.isdir(os.path.join(params[1], colors[i])):
            os.makedirs(os.path.join(params[1], colors[i]))
        hdu.header['OBJECT'] = params[3]
        hdu.header['EXPOSURE'] = str(tags['EXIF ExposureTime'])
        date = dt.strptime(str(tags['EXIF DateTimeOriginal']), "%Y:%m:%d %X")  # 2019:02:05 19:29:30
        hdu.header['DATE'] = date.strftime("%Y-%m-%dT%X")  # 2019-02-05T19:29:30
        hdu.writeto(open(path, "wb"))
        print(path)


if __name__ == "__main__":
    files = os.listdir(params[0])
    pool = Pool(8)  # Number of threads
    pool.map(partial(process, params=params), files)
    pool.close()
    pool.join()
