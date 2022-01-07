import os
import numpy as np
import pandas as pd
from osgeo import gdal
import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1 import make_axes_locatable
from matplotlib.lines import Line2D
import geopandas as gpd
from sklearn import metrics
import math
import glob

YIELD_PTH = "/beegfs/jaenicke/klimertrag/raster/sim-yields/testing_difference_to_anna/different_setups"
RUN_IDS =[
    # 142,143,144,145,
    # 146,147,148,149,
    151,152,153,154,
    # 155,156,157,158,
    # 159,160,161,162,
    # 163,164,165,166,
    # 167,168,169,170, 
    # 172,173,174,175]
    # 176,177,178,179,
    # 180,181,182,183,
    # 184,185,186,187,
    # 188,189,190,191,
    # 192,193,194,195,
    # 196,197,198,199,
    # 200,201,202,203,
    # 204,205,206,207,
    # 208,209,210,211,
    # 221,222,223,224,
    # 225,226,227,228,
    # 229,230,231,232,
    # 253,254,255,256,
    # 257,258,259,260,
    # 261,262,263,264
]

RUN_ID_DICT = {
    142 : [list(range(1980, 2011))],##
    143 : [list(range(2035, 2066))],
    144 : [list(range(2035, 2066))],
    145 : [list(range(2035, 2066))],

    146 : [list(range(1980, 2011))],##
    147 : [list(range(2035, 2066))],
    148 : [list(range(2035, 2066))],
    149 : [list(range(2035, 2066))],

    151 : [list(range(1980, 2011))],##
    152 : [list(range(2035, 2066))],    
    153 : [list(range(2035, 2066))],
    154 : [list(range(2035, 2066))],

    155 : [list(range(1980, 2011))],##
    156 : [list(range(2035, 2066))],
    157 : [list(range(2035, 2066))],
    158 : [list(range(2035, 2066))],

    159 : [list(range(1980, 2011))],##
    160 : [list(range(2035, 2066))],
    161 : [list(range(2035, 2066))],
    162 : [list(range(2035, 2066))],

    163 : [list(range(1980, 2011))],##
    164 : [list(range(2035, 2066))],
    165 : [list(range(2035, 2066))],
    166 : [list(range(2035, 2066))],

    167 : [list(range(1980, 2011))],##
    168 : [list(range(2035, 2066))],
    169 : [list(range(2035, 2066))],
    170 : [list(range(2035, 2066))],

    172 : [list(range(1980, 2011))],##
    173 : [list(range(2035, 2066))],
    174 : [list(range(2035, 2066))],
    175 : [list(range(2035, 2066))],

    176 : [list(range(1980, 2011))],##
    177 : [list(range(2035, 2066))],
    178 : [list(range(2035, 2066))],
    179 : [list(range(2035, 2066))],

    180 : [list(range(1980, 2011))],##
    181 : [list(range(2035, 2066))],
    182 : [list(range(2035, 2066))],
    183 : [list(range(2035, 2066))],
    
    184 : [list(range(1980, 2011))],##
    185 : [list(range(2035, 2066))],
    186 : [list(range(2035, 2066))],
    187 : [list(range(2035, 2066))],

    188 : [list(range(1980, 2011))],##
    189 : [list(range(2035, 2066))],
    190 : [list(range(2035, 2066))],
    191 : [list(range(2035, 2066))],

    192 : [list(range(1980, 2011))],##
    193 : [list(range(2035, 2066))],
    194 : [list(range(2035, 2066))],
    195 : [list(range(2035, 2066))],

    196 : [list(range(1980, 2011))],##
    197 : [list(range(2035, 2066))],
    198 : [list(range(2035, 2066))],
    199 : [list(range(2035, 2066))],

    200 : [list(range(1980, 2011))],##
    201 : [list(range(2035, 2066))],
    202 : [list(range(2035, 2066))],
    203 : [list(range(2035, 2066))],

    204 : [list(range(1980, 2011))],##
    205 : [list(range(2035, 2066))],
    206 : [list(range(2035, 2066))],
    207 : [list(range(2035, 2066))],

    208 : [list(range(1980, 2011))],##
    209 : [list(range(2035, 2066))],
    210 : [list(range(2035, 2066))],
    211 : [list(range(2035, 2066))],

    221 : [list(range(1980, 2011))],##
    222 : [list(range(2035, 2066))],
    223 : [list(range(2035, 2066))],
    224 : [list(range(2035, 2066))],

    225 : [list(range(1980, 2011))],##
    226 : [list(range(2035, 2066))],
    227 : [list(range(2035, 2066))],
    228 : [list(range(2035, 2066))],

    229 : [list(range(1980, 2011))],##
    230 : [list(range(2035, 2066))],
    231 : [list(range(2035, 2066))],
    232 : [list(range(2035, 2066))],

    253 : [list(range(1980, 2011))],##
    254 : [list(range(2035, 2066))],
    255 : [list(range(2035, 2066))],
    256 : [list(range(2035, 2066))],

    257 : [list(range(1980, 2011))],##
    258 : [list(range(2035, 2066))],
    259 : [list(range(2035, 2066))],
    260 : [list(range(2035, 2066))],

    261 : [list(range(1980, 2011))],##
    262 : [list(range(2035, 2066))],
    263 : [list(range(2035, 2066))],
    264 : [list(range(2035, 2066))],
}

def cm2inch(*tupl):
    inch = 2.54
    if isinstance(tupl[0], tuple):
        return tuple(i/inch for i in tupl[0])
    else:
        return tuple(i/inch for i in tupl)

def write_array_to_raster(in_array,
                          out_path,
                          gt,
                          pr,
                          no_data_value,
                          type_code=None,
                          options=['COMPRESS=DEFLATE', 'PREDICTOR=1'],
                          driver='GTiff'):
    """
    Writes an array to a tiff-raster. If no type code of output is given, it will be extracted from the input array.
    As default a deflate compression is used, but can be specified by the user.
    :param in_array: Input array
    :param out_path: Path of output raster
    :param gt: GeoTransfrom of output raster
    :param pr: Projection of output raster
    :param no_data_value: Value that should be recognized as the no data value
    :return: Writes an array to a raster file on the disc.
    """

    from osgeo import gdal
    from osgeo import gdal_array

    if type_code == None:
        type_code = gdal_array.NumericTypeCodeToGDALTypeCode(in_array.dtype)

    if len(in_array.shape) == 3:
        nbands_out = in_array.shape[0]
        x_res = in_array.shape[2]
        y_res = in_array.shape[1]

        out_ras = gdal.GetDriverByName(driver).Create(out_path, x_res, y_res, nbands_out, type_code, options=options)
        out_ras.SetGeoTransform(gt)
        out_ras.SetProjection(pr)

        for b in range(0, nbands_out):
            band = out_ras.GetRasterBand(b + 1)
            arr_out = in_array[b, :, :]
            band.WriteArray(arr_out)
            band.SetNoDataValue(no_data_value)
            band.FlushCache()

        del (out_ras)

    if len(in_array.shape) == 2:
        nbands_out = 1
        x_res = in_array.shape[1]
        y_res = in_array.shape[0]

        out_ras = gdal.GetDriverByName(driver).Create(out_path, x_res, y_res, nbands_out, type_code, options=options)
        out_ras.SetGeoTransform(gt)
        out_ras.SetProjection(pr)

        band = out_ras.GetRasterBand(1)
        band.WriteArray(in_array)
        band.SetNoDataValue(no_data_value)
        band.FlushCache()

        del (out_ras)

def create_avg_raster_over_years(variable_name, run_id):

    yield_pth = f"{YIELD_PTH}/{run_id}"

    if not os.path.exists(yield_pth):
        print(f"Path to yield simulations with run-ID {run_id} does not exist. \n {yield_pth}. \n")
        return

    ## Get crop name of current run ID
    crop_name_dict = {
        'wheatwinterwheat':'WW',
        'barleywinterbarley':'WB',
        'ryewinterrye':'WRye',
        'maizesilagemaize':'SM',
        'sugarbeet':'SU',
        'potatomoderatelyearlypotato':'PO',
        'rapewinterrape':'WR',
        'barleyspringbarley':'SB'
    }
    search_term = rf'{yield_pth}/*{variable_name}*.asc'
    file_lst = glob.glob(search_term)
    if file_lst:
        file_pth = file_lst[0]
        crop_name = os.path.basename(file_pth).split('_')[0]
        crop = crop_name_dict[crop_name]
    else:
        return

    ## Get year list from global variables
    year_lst = RUN_ID_DICT[run_id][0]
    yield_arr_lst = []

    for year in year_lst:
        print(year, crop)
        ## list all files of the current run
        search_term = rf'{yield_pth}/{crop_name}_{variable_name}_{year}*.asc'
        file_pth = glob.glob(search_term)
        if file_pth:
            file_pth = file_pth[0]
        else:
            print(f'No file for year {year} found.')
            continue

        print(file_pth)

        ## Open simulated crop yields
        yield_ras = gdal.Open(file_pth)
        yield_arr = yield_ras.ReadAsArray()
        ndv_yields = yield_ras.GetRasterBand(1).GetNoDataValue()

        ## Create a mask that sets all no data values to 0
        ndv_mask = np.where(yield_arr == ndv_yields, 0, 1)

        ## Mask yield array with no data mask
        yield_arr_m = np.ma.masked_where(ndv_mask == 0, yield_arr)

        yield_arr_lst.append(yield_arr_m.copy())

    ## Average yields over all years
    yields_aggr = np.ma.mean(yield_arr_lst, axis=0)

    gt = yield_ras.GetGeoTransform()
    pr = yield_ras.GetProjection()

    min_year = min(year_lst)
    max_year = max(year_lst)
    out_pth = fr"{YIELD_PTH}/{crop}_{variable_name}-avg_{min_year}-{max_year}_{run_id}.tif"

    write_array_to_raster(yields_aggr,
                        out_path=out_pth,
                        gt=gt,
                        pr=pr,
                        no_data_value=ndv_yields,
                        type_code=None,
                        options=['COMPRESS=DEFLATE', 'PREDICTOR=1'],
                        driver='GTiff')

def calculate_avg_from_list(file_lst, out_pth):
    yield_arr_lst = []
    for file_pth in file_lst:
        print(file_pth)

        ## Open simulated crop yields
        yield_ras = gdal.Open(file_pth)
        yield_arr = yield_ras.ReadAsArray()
        ndv_yields = yield_ras.GetRasterBand(1).GetNoDataValue()

        ## Create a mask that sets all no data values to 0
        ndv_mask = np.where(yield_arr == ndv_yields, 0, 1)

        ## Mask yield array with no data mask
        yield_arr_m = np.ma.masked_where(ndv_mask == 0, yield_arr)

        yield_arr_lst.append(yield_arr_m.copy())

    ## Average yields over all years
    yields_aggr = np.ma.mean(yield_arr_lst, axis=0)

    gt = yield_ras.GetGeoTransform()
    pr = yield_ras.GetProjection()

    write_array_to_raster(yields_aggr,
                        out_path=out_pth,
                        gt=gt,
                        pr=pr,
                        no_data_value=ndv_yields,
                        type_code=None,
                        options=['COMPRESS=DEFLATE', 'PREDICTOR=1'],
                        driver='GTiff')

def calculate_difference_between_rasters(arr1, arr2, no_data_value, percentage_change=True):
    """
    Calculates difference between 1-band arrays (arr2 - arr1). Output can be total change or %-change.
    """
    if arr1.ndim > 2:
        print("Array 1 has more than 2 dimensions! Only two dimensions allowed!")
        return

    if arr2.ndim > 2:
        print("Array 2 has more than 2 dimensions!  Only two dimensions allowed!")
        return

    if arr1.shape != arr2.shape:
        print("Provided array have different shapes. Please provide array with same shapes and only 1 band.")
        return

    arr1_m = np.where(arr1 == 0.0, 0, 1)
    arr2_m = np.where(arr2 == 0.0, 0, 1)

    mask = arr1_m * arr2_m

    diff_arr = arr2 - arr1

    if percentage_change:
        diff_arr = np.divide(diff_arr, arr1) * 100

    diff_arr = np.where(mask == 1, diff_arr, no_data_value)

    return diff_arr

def plot_difference_raster(diff_arr, ignore_value, title, out_pth, v_min, v_max):
    diff_arr = np.where(diff_arr == ignore_value, 0, diff_arr)

    font_size = 12
    plt.rcParams['legend.title_fontsize'] = f'{font_size}'
    plt.rcParams["font.family"] = "Calibri"

    fig, ax = plt.subplots(figsize=cm2inch(25.0, 30))
    img = ax.imshow(diff_arr, cmap=plt.cm.RdYlGn)
    ax.set_title(title, loc='left', fontsize=font_size)
    img.set_clim(vmin=v_min, vmax=v_max)
    fig.colorbar(img, ax=ax)

    plt.tight_layout()
    plt.savefig(out_pth)


def main(): 
    #####################################################################
    #####################################################################
    ### Calculate the average of specific time periods (indicated by run id). Year list is provided in CROP dictionary.
    for run_id in RUN_IDS:
        print(run_id)
        create_avg_raster_over_years(variable_name='Yield', run_id=run_id)

    #####################################################################
    #####################################################################

    #### Combine several time periods by averaging.
    folder  = "/beegfs/jaenicke/klimertrag/raster/sim-yields/testing_difference_to_anna/different_setups"

    # #####################################################################
    # ## Klimertrag parameters + no fertilization + automatic sowing & harvest + vernalization fix on + klima: dwd_core_ensemble
    # file_lst = [
    #     f"{folder}/WW_Yield-avg_2035-2065_143.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_144.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_145.tif"
    # ]
    # out_pth = f"{folder}/WW_Yield-avg_2035-2036_klim_nofert_sauto_hauto_vernon_dwdcore.tif"
    # calculate_avg_from_list(file_lst, out_pth)

    # arr1 = gdal.Open(f"{folder}/WW_Yield-avg_1980-2010_142.tif").ReadAsArray()
    # ras2 = gdal.Open(out_pth)
    # arr2 = ras2.ReadAsArray()
    # gt = ras2.GetGeoTransform()
    # pr = ras2.GetProjection()
    # diff_arr = calculate_difference_between_rasters(arr1, arr2, no_data_value = -9999, percentage_change=True)
    
    # out_pth = f"{folder}/yield_diff-35_65-80_10-klim-nofert-sauto_hauto_vernon_dwdcore.tif"
    # write_array_to_raster(diff_arr,
    #                 out_path=out_pth,
    #                 gt=gt,
    #                 pr=pr,
    #                 no_data_value=-9999,
    #                 type_code=None,
    #                 options=['COMPRESS=DEFLATE', 'PREDICTOR=1'],
    #                 driver='GTiff')

    # title = "yield_diff-35_65-80_10-klim-nofert-sauto_hauto_vernon_dwdcore"
    # out_pth = f"/beegfs/jaenicke/klimertrag/figures/08_testing_difference_to_anna/difference/{title}.png"
    # plot_difference_raster(diff_arr, -9999, title, out_pth, -25, 25)

    # #####################################################################
    # ## Klimertrag parameters + fertilization + fixed sowing and harvest at specific date + vernalization fix on + klima: dwd_core_ensemble
    # file_lst = [
    #     f"{folder}/WW_Yield-avg_2035-2065_147.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_148.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_149.tif"
    # ]
    # out_pth = f"{folder}/WW_Yield-avg_2035-2036_klim_fert_sfix1_hfix1_vernon_dwdcore.tif"
    # calculate_avg_from_list(file_lst, out_pth)

    # arr1 = gdal.Open(f"{folder}/WW_Yield-avg_1980-2010_146.tif").ReadAsArray()
    # ras2 = gdal.Open(out_pth)
    # arr2 = ras2.ReadAsArray()
    # gt = ras2.GetGeoTransform()
    # pr = ras2.GetProjection()
    # diff_arr = calculate_difference_between_rasters(arr1, arr2, no_data_value = -9999, percentage_change=True)
    
    # out_pth = f"{folder}/yield_diff-35_65-80_10-klim_fert_sfix1_hfix1_vernon_dwdcore.tif"
    # write_array_to_raster(diff_arr,
    #                 out_path=out_pth,
    #                 gt=gt,
    #                 pr=pr,
    #                 no_data_value=-9999,
    #                 type_code=None,
    #                 options=['COMPRESS=DEFLATE', 'PREDICTOR=1'],
    #                 driver='GTiff')

    # title = "yield_diff-35_65-80_10-klim_fert_sfix1_hfix1_vernon_dwdcore"
    # out_pth = f"/beegfs/jaenicke/klimertrag/figures/08_testing_difference_to_anna/difference/{title}.png"
    # plot_difference_raster(diff_arr, -9999, title, out_pth,  -25, 25)

    #####################################################################
    ## Sustag parameters + fertilization + fixed sowing and harvest at specific date + vernalization fix on + klima: dwd_core_ensemble * co2=0 + 10 years forerun
    descr = "sust_fert_sfix1_hfix1_vernon_dwdcore_co20_10years"
    file_lst = [
        f"{folder}/WW_Yield-avg_2035-2065_152.tif",
        f"{folder}/WW_Yield-avg_2035-2065_153.tif",
        f"{folder}/WW_Yield-avg_2035-2065_154.tif"
    ]
    out_pth = f"{folder}/WW_Yield-avg_2035-2036_{descr}.tif"
    calculate_avg_from_list(file_lst, out_pth)

    arr1 = gdal.Open(f"{folder}/WW_Yield-avg_1980-2010_151.tif").ReadAsArray()
    ras2 = gdal.Open(out_pth)
    arr2 = ras2.ReadAsArray()
    gt = ras2.GetGeoTransform()
    pr = ras2.GetProjection()
    diff_arr = calculate_difference_between_rasters(arr1, arr2, no_data_value = -9999, percentage_change=True)
    
    out_pth = f"{folder}/yield_diff-35_65-80_10-{descr}.tif"
    write_array_to_raster(diff_arr,
                    out_path=out_pth,
                    gt=gt,
                    pr=pr,
                    no_data_value=-9999,
                    type_code=None,
                    options=['COMPRESS=DEFLATE', 'PREDICTOR=1'],
                    driver='GTiff')

    title = f"yield_diff-35_65-80_10-{descr}"
    out_pth = f"/beegfs/jaenicke/klimertrag/figures/08_testing_difference_to_anna/difference 2/{title}.png"
    plot_difference_raster(diff_arr, -9999, title, out_pth,  -25, 25)

    # #####################################################################
    # ## KlimErtrag parameters + fertilization + automatic sowing and harvest + vernalization fix on + klima: dwd_core_ensemble
    # file_lst = [
    #     f"{folder}/WW_Yield-avg_2035-2065_156.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_157.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_158.tif"
    # ]
    # out_pth = f"{folder}/WW_Yield-avg_2035-2036_klim_fert_sauto_hauto_vernon_dwdcore.tif"
    # calculate_avg_from_list(file_lst, out_pth)

    # arr1 = gdal.Open(f"{folder}/WW_Yield-avg_1980-2010_155.tif").ReadAsArray()
    # ras2 = gdal.Open(out_pth)
    # arr2 = ras2.ReadAsArray()
    # gt = ras2.GetGeoTransform()
    # pr = ras2.GetProjection()
    # diff_arr = calculate_difference_between_rasters(arr1, arr2, no_data_value = -9999, percentage_change=True)
    
    # out_pth = f"{folder}/yield_diff-35_65-80_10-klim_fert_sauto_hauto_vernon_dwdcore.tif"
    # write_array_to_raster(diff_arr,
    #                 out_path=out_pth,
    #                 gt=gt,
    #                 pr=pr,
    #                 no_data_value=-9999,
    #                 type_code=None,
    #                 options=['COMPRESS=DEFLATE', 'PREDICTOR=1'],
    #                 driver='GTiff')

    # title = "yield_diff-35_65-80_10-klim_fert_sauto_hauto_vernon_dwdcore"
    # out_pth = f"/beegfs/jaenicke/klimertrag/figures/08_testing_difference_to_anna/difference/{title}.png"
    # plot_difference_raster(diff_arr, -9999, title, out_pth,  -25, 25)

    # #####################################################################
    # ## Sustag parameters + fertilization + fixed sowing and harvest at specific date + vernalization fix off + klima: dwd_core_ensemble + c02 = 0 + 10years forerun
    # descr = "sust_fert_sfix1_hfix1_vernoff_dwdcore_co20_10years"
    # file_lst = [
    #     f"{folder}/WW_Yield-avg_2035-2065_160.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_161.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_162.tif"
    # ]
    # out_pth = f"{folder}/WW_Yield-avg_2035-2036_{descr}.tif"
    # calculate_avg_from_list(file_lst, out_pth)

    # arr1 = gdal.Open(f"{folder}/WW_Yield-avg_1980-2010_159.tif").ReadAsArray()
    # ras2 = gdal.Open(out_pth)
    # arr2 = ras2.ReadAsArray()
    # gt = ras2.GetGeoTransform()
    # pr = ras2.GetProjection()
    # diff_arr = calculate_difference_between_rasters(arr1, arr2, no_data_value = -9999, percentage_change=True)
    
    # out_pth = f"{folder}/yield_diff-35_65-80_10-{descr}.tif"
    # write_array_to_raster(diff_arr,
    #                 out_path=out_pth,
    #                 gt=gt,
    #                 pr=pr,
    #                 no_data_value=-9999,
    #                 type_code=None,
    #                 options=['COMPRESS=DEFLATE', 'PREDICTOR=1'],
    #                 driver='GTiff')

    # title = f"yield_diff-35_65-80_10-{descr}"
    # out_pth = f"/beegfs/jaenicke/klimertrag/figures/08_testing_difference_to_anna/difference/{title}.png"
    # plot_difference_raster(diff_arr, -9999, title, out_pth,  -25, 25)

    # #####################################################################
    # ## Sustag parameters + fertilization + fixed sowing and harvest at specific date + vernalization fix on + klima: cmip_cordex_reklies
    # file_lst = [
    #     f"{folder}/WW_Yield-avg_2035-2065_164.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_165.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_166.tif"
    # ]
    # out_pth = f"{folder}/WW_Yield-avg_2035-2036_sust_fert_sfix1_hfix1_vernon_cmip.tif"
    # calculate_avg_from_list(file_lst, out_pth)

    # arr1 = gdal.Open(f"{folder}/WW_Yield-avg_1980-2010_163.tif").ReadAsArray()
    # ras2 = gdal.Open(out_pth)
    # arr2 = ras2.ReadAsArray()
    # gt = ras2.GetGeoTransform()
    # pr = ras2.GetProjection()
    # diff_arr = calculate_difference_between_rasters(arr1, arr2, no_data_value = -9999, percentage_change=True)
    
    # out_pth = f"{folder}/yield_diff-35_65-80_10-sust_fert_sfix1_hfix1_vernon_cmip.tif"
    # write_array_to_raster(diff_arr,
    #                 out_path=out_pth,
    #                 gt=gt,
    #                 pr=pr,
    #                 no_data_value=-9999,
    #                 type_code=None,
    #                 options=['COMPRESS=DEFLATE', 'PREDICTOR=1'],
    #                 driver='GTiff')

    # title = "yield_diff-35_65-80_10-sust_fert_sfix1_hfix1_vernon_cmip"
    # out_pth = f"/beegfs/jaenicke/klimertrag/figures/08_testing_difference_to_anna/difference/{title}.png"
    # plot_difference_raster(diff_arr, -9999, title, out_pth,  -25, 25)

    # #####################################################################
    # ## Klimertrag parameters + no fertilization + auto sowing and harvest at specific date + vernalization fix on + klima: cmip_cordex_reklies
    # file_lst = [
    #     f"{folder}/WW_Yield-avg_2035-2065_168.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_169.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_170.tif"
    # ]
    # out_pth = f"{folder}/WW_Yield-avg_2035-2036_klim_nofert_sauto_hauto_vernon_cmip.tif"
    # calculate_avg_from_list(file_lst, out_pth)

    # arr1 = gdal.Open(f"{folder}/WW_Yield-avg_1980-2010_167.tif").ReadAsArray()
    # ras2 = gdal.Open(out_pth)
    # arr2 = ras2.ReadAsArray()
    # gt = ras2.GetGeoTransform()
    # pr = ras2.GetProjection()
    # diff_arr = calculate_difference_between_rasters(arr1, arr2, no_data_value = -9999, percentage_change=True)
    
    # out_pth = f"{folder}/yield_diff-35_65-80_10-klim_nofert_sauto_hauto_vernon_cmip.tif"
    # write_array_to_raster(diff_arr,
    #                 out_path=out_pth,
    #                 gt=gt,
    #                 pr=pr,
    #                 no_data_value=-9999,
    #                 type_code=None,
    #                 options=['COMPRESS=DEFLATE', 'PREDICTOR=1'],
    #                 driver='GTiff')

    # title = "yield_diff-35_65-80_10-klim_nofert_sauto_hauto_vernon_cmip"
    # out_pth = f"/beegfs/jaenicke/klimertrag/figures/08_testing_difference_to_anna/difference/{title}.png"
    # plot_difference_raster(diff_arr, -9999, title, out_pth,  -25, 25)

    # #####################################################################
    # ## Klimertrag parameters + no fertilization + automatic sowing & harvest + vernalization fix off + klima: dwd_core_ensemble
    # file_lst = [
    #     f"{folder}/WW_Yield-avg_2035-2065_173.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_174.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_175.tif"
    # ]
    # out_pth = f"{folder}/WW_Yield-avg_2035-2036_klim_nofert_sauto_hauto_vernoff_dwdcore.tif"
    # calculate_avg_from_list(file_lst, out_pth)

    # arr1 = gdal.Open(f"{folder}/WW_Yield-avg_1980-2010_172.tif").ReadAsArray()
    # ras2 = gdal.Open(out_pth)
    # arr2 = ras2.ReadAsArray()
    # gt = ras2.GetGeoTransform()
    # pr = ras2.GetProjection()
    # diff_arr = calculate_difference_between_rasters(arr1, arr2, no_data_value = -9999, percentage_change=True)
    
    # out_pth = f"{folder}/yield_diff-35_65-80_10-klim_nofert_sauto_hauto_vernoff_dwdcore.tif"
    # write_array_to_raster(diff_arr,
    #                 out_path=out_pth,
    #                 gt=gt,
    #                 pr=pr,
    #                 no_data_value=-9999,
    #                 type_code=None,
    #                 options=['COMPRESS=DEFLATE', 'PREDICTOR=1'],
    #                 driver='GTiff')

    # title = "yield_diff-35_65-80_10-klim_nofert_sauto_hauto_vernoff_dwdcore"
    # out_pth = f"/beegfs/jaenicke/klimertrag/figures/08_testing_difference_to_anna/difference/{title}.png"
    # plot_difference_raster(diff_arr, -9999, title, out_pth, -25, 25)

    # #####################################################################
    # ## Klimertrag parameters + no fertilization + automatic sowing & harvest + vernalization fix off + klima: dwd_core_ensemble + co2 = 0 + 10 years forerun
    # descr = 'klim_nofert_sauto_hauto_vernoff_dwdcore_co20_10years'
    # file_lst = [
    #     f"{folder}/WW_Yield-avg_2035-2065_177.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_178.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_179.tif"
    # ]
    # out_pth = f"{folder}/WW_Yield-avg_2035-2036_{descr}.tif"
    # calculate_avg_from_list(file_lst, out_pth)

    # arr1 = gdal.Open(f"{folder}/WW_Yield-avg_1980-2010_176.tif").ReadAsArray()
    # ras2 = gdal.Open(out_pth)
    # arr2 = ras2.ReadAsArray()
    # gt = ras2.GetGeoTransform()
    # pr = ras2.GetProjection()
    # diff_arr = calculate_difference_between_rasters(arr1, arr2, no_data_value = -9999, percentage_change=True)
    
    # out_pth = f"{folder}/yield_diff-35_65-80_10-{descr}.tif"
    # write_array_to_raster(diff_arr,
    #                 out_path=out_pth,
    #                 gt=gt,
    #                 pr=pr,
    #                 no_data_value=-9999,
    #                 type_code=None,
    #                 options=['COMPRESS=DEFLATE', 'PREDICTOR=1'],
    #                 driver='GTiff')

    # title = f"yield_diff-35_65-80_10-{descr}"
    # out_pth = f"/beegfs/jaenicke/klimertrag/figures/08_testing_difference_to_anna/difference/{title}.png"
    # plot_difference_raster(diff_arr, -9999, title, out_pth, -25, 25)

    # #####################################################################
    # ## Klimertrag parameters + no fertilization + automatic sowing & harvest + vernalization fix off + klima: dwd_core_ensemble + co2 = 0 + 1 years forerun
    # descr = 'klim_nofert_sauto_hauto_vernoff_dwdcore_co20_1year'
    # file_lst = [
    #     f"{folder}/WW_Yield-avg_2035-2065_181.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_182.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_183.tif"
    # ]
    # out_pth = f"{folder}/WW_Yield-avg_2035-2036_{descr}.tif"
    # calculate_avg_from_list(file_lst, out_pth)

    # arr1 = gdal.Open(f"{folder}/WW_Yield-avg_1980-2010_180.tif").ReadAsArray()
    # ras2 = gdal.Open(out_pth)
    # arr2 = ras2.ReadAsArray()
    # gt = ras2.GetGeoTransform()
    # pr = ras2.GetProjection()
    # diff_arr = calculate_difference_between_rasters(arr1, arr2, no_data_value = -9999, percentage_change=True)
    
    # out_pth = f"{folder}/yield_diff-35_65-80_10-{descr}.tif"
    # write_array_to_raster(diff_arr,
    #                 out_path=out_pth,
    #                 gt=gt,
    #                 pr=pr,
    #                 no_data_value=-9999,
    #                 type_code=None,
    #                 options=['COMPRESS=DEFLATE', 'PREDICTOR=1'],
    #                 driver='GTiff')

    # title = f"yield_diff-35_65-80_10-{descr}"
    # out_pth = f"/beegfs/jaenicke/klimertrag/figures/08_testing_difference_to_anna/difference/{title}.png"
    # plot_difference_raster(diff_arr, -9999, title, out_pth, -25, 25)

    # #####################################################################
    # ## Sustag parameters + fertilization + fixed sowing & harvest at one date + vernalization fix off + klima: dwd_core_ensemble + co2 = 0 + 1 years forerun
    # descr = 'sust_fert_sfix1_hfix1_vernoff_dwdcore_co20_1year'
    # file_lst = [
    #     f"{folder}/WW_Yield-avg_2035-2065_185.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_186.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_187.tif"
    # ]
    # out_pth = f"{folder}/WW_Yield-avg_2035-2036_{descr}.tif"
    # calculate_avg_from_list(file_lst, out_pth)

    # arr1 = gdal.Open(f"{folder}/WW_Yield-avg_1980-2010_184.tif").ReadAsArray()
    # ras2 = gdal.Open(out_pth)
    # arr2 = ras2.ReadAsArray()
    # gt = ras2.GetGeoTransform()
    # pr = ras2.GetProjection()
    # diff_arr = calculate_difference_between_rasters(arr1, arr2, no_data_value = -9999, percentage_change=True)
    
    # out_pth = f"{folder}/yield_diff-35_65-80_10-{descr}.tif"
    # write_array_to_raster(diff_arr,
    #                 out_path=out_pth,
    #                 gt=gt,
    #                 pr=pr,
    #                 no_data_value=-9999,
    #                 type_code=None,
    #                 options=['COMPRESS=DEFLATE', 'PREDICTOR=1'],
    #                 driver='GTiff')

    # title = f"yield_diff-35_65-80_10-{descr}"
    # out_pth = f"/beegfs/jaenicke/klimertrag/figures/08_testing_difference_to_anna/difference/{title}.png"
    # plot_difference_raster(diff_arr, -9999, title, out_pth, -25, 25)

    # #####################################################################
    # ## Sustag parameters + fertilization + fixed sowing & harvest at one date + vernalization fix off + klima: dwd_core_ensemble + co2 = empty + 1 years forerun
    # descr = 'sust_fert_sfix1_hfix1_vernoff_dwdcore_co2empty_1year'
    # file_lst = [
    #     f"{folder}/WW_Yield-avg_2035-2065_189.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_190.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_191.tif"
    # ]
    # out_pth = f"{folder}/WW_Yield-avg_2035-2036_{descr}.tif"
    # calculate_avg_from_list(file_lst, out_pth)

    # arr1 = gdal.Open(f"{folder}/WW_Yield-avg_1980-2010_188.tif").ReadAsArray()
    # ras2 = gdal.Open(out_pth)
    # arr2 = ras2.ReadAsArray()
    # gt = ras2.GetGeoTransform()
    # pr = ras2.GetProjection()
    # diff_arr = calculate_difference_between_rasters(arr1, arr2, no_data_value = -9999, percentage_change=True)
    
    # out_pth = f"{folder}/yield_diff-35_65-80_10-{descr}.tif"
    # write_array_to_raster(diff_arr,
    #                 out_path=out_pth,
    #                 gt=gt,
    #                 pr=pr,
    #                 no_data_value=-9999,
    #                 type_code=None,
    #                 options=['COMPRESS=DEFLATE', 'PREDICTOR=1'],
    #                 driver='GTiff')

    # title = f"yield_diff-35_65-80_10-{descr}"
    # out_pth = f"/beegfs/jaenicke/klimertrag/figures/08_testing_difference_to_anna/difference/{title}.png"
    # plot_difference_raster(diff_arr, -9999, title, out_pth, -25, 25)

    # #####################################################################
    # ## Sustag parameters + fertilization + fixed sowing & harvest at one date + vernalization fix off + klima: dwd_core_ensemble + co2 = empty + 10 years forerun
    # descr = 'sust_fert_sfix1_hfix1_vernoff_dwdcore_co2empty_10years'
    # file_lst = [
    #     f"{folder}/WW_Yield-avg_2035-2065_193.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_194.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_195.tif"
    # ]
    # out_pth = f"{folder}/WW_Yield-avg_2035-2036_{descr}.tif"
    # calculate_avg_from_list(file_lst, out_pth)

    # arr1 = gdal.Open(f"{folder}/WW_Yield-avg_1980-2010_192.tif").ReadAsArray()
    # ras2 = gdal.Open(out_pth)
    # arr2 = ras2.ReadAsArray()
    # gt = ras2.GetGeoTransform()
    # pr = ras2.GetProjection()
    # diff_arr = calculate_difference_between_rasters(arr1, arr2, no_data_value = -9999, percentage_change=True)
    
    # out_pth = f"{folder}/yield_diff-35_65-80_10-{descr}.tif"
    # write_array_to_raster(diff_arr,
    #                 out_path=out_pth,
    #                 gt=gt,
    #                 pr=pr,
    #                 no_data_value=-9999,
    #                 type_code=None,
    #                 options=['COMPRESS=DEFLATE', 'PREDICTOR=1'],
    #                 driver='GTiff')

    # title = f"yield_diff-35_65-80_10-{descr}"
    # out_pth = f"/beegfs/jaenicke/klimertrag/figures/08_testing_difference_to_anna/difference/{title}.png"
    # plot_difference_raster(diff_arr, -9999, title, out_pth, -25, 25)

    # #####################################################################
    # ## Klimertrag parameters + fertilization + fixed sowing & harvest at one date + vernalization fix off + klima: dwd_core_ensemble + co2 = empty + 1 year forerun
    # descr = 'klim_fert_sfix1_hfix1_vernoff_dwdcore_co2empty_1year'
    # file_lst = [
    #     f"{folder}/WW_Yield-avg_2035-2065_197.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_198.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_199.tif"
    # ]
    # out_pth = f"{folder}/WW_Yield-avg_2035-2036_{descr}.tif"
    # calculate_avg_from_list(file_lst, out_pth)

    # arr1 = gdal.Open(f"{folder}/WW_Yield-avg_1980-2010_196.tif").ReadAsArray()
    # ras2 = gdal.Open(out_pth)
    # arr2 = ras2.ReadAsArray()
    # gt = ras2.GetGeoTransform()
    # pr = ras2.GetProjection()
    # diff_arr = calculate_difference_between_rasters(arr1, arr2, no_data_value = -9999, percentage_change=True)
    
    # out_pth = f"{folder}/yield_diff-35_65-80_10-{descr}.tif"
    # write_array_to_raster(diff_arr,
    #                 out_path=out_pth,
    #                 gt=gt,
    #                 pr=pr,
    #                 no_data_value=-9999,
    #                 type_code=None,
    #                 options=['COMPRESS=DEFLATE', 'PREDICTOR=1'],
    #                 driver='GTiff')

    # title = f"yield_diff-35_65-80_10-{descr}"
    # out_pth = f"/beegfs/jaenicke/klimertrag/figures/08_testing_difference_to_anna/difference/{title}.png"
    # plot_difference_raster(diff_arr, -9999, title, out_pth, -25, 25)

    # #####################################################################
    # ## Klimertrag parameters + fertilization + fixed sowing & harvest at one date + vernalization fix off + klima: dwd_core_ensemble + co2 = empty + 10 years forerun
    # descr = 'klim_fert_sfix1_hfix1_vernoff_dwdcore_co2empty_10years'
    # file_lst = [
    #     f"{folder}/WW_Yield-avg_2035-2065_201.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_202.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_203.tif"
    # ]
    # out_pth = f"{folder}/WW_Yield-avg_2035-2036_{descr}.tif"
    # calculate_avg_from_list(file_lst, out_pth)

    # arr1 = gdal.Open(f"{folder}/WW_Yield-avg_1980-2010_200.tif").ReadAsArray()
    # ras2 = gdal.Open(out_pth)
    # arr2 = ras2.ReadAsArray()
    # gt = ras2.GetGeoTransform()
    # pr = ras2.GetProjection()
    # diff_arr = calculate_difference_between_rasters(arr1, arr2, no_data_value = -9999, percentage_change=True)
    
    # out_pth = f"{folder}/yield_diff-35_65-80_10-{descr}.tif"
    # write_array_to_raster(diff_arr,
    #                 out_path=out_pth,
    #                 gt=gt,
    #                 pr=pr,
    #                 no_data_value=-9999,
    #                 type_code=None,
    #                 options=['COMPRESS=DEFLATE', 'PREDICTOR=1'],
    #                 driver='GTiff')

    # title = f"yield_diff-35_65-80_10-{descr}"
    # out_pth = f"/beegfs/jaenicke/klimertrag/figures/08_testing_difference_to_anna/difference/{title}.png"
    # plot_difference_raster(diff_arr, -9999, title, out_pth, -25, 25)

    # #####################################################################
    # ## Klimertrag parameters + fertilization + fixed sowing & harvest at one date + vernalization fix on + klima: dwd_core_ensemble + co2 = empty + 1 year forerun
    # descr = 'klim_fert_sfix1_hfix1_vernon_dwdcore_co2empty_1year'
    # file_lst = [
    #     f"{folder}/WW_Yield-avg_2035-2065_209.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_210.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_211.tif"
    # ]
    # out_pth = f"{folder}/WW_Yield-avg_2035-2036_{descr}.tif"
    # calculate_avg_from_list(file_lst, out_pth)

    # arr1 = gdal.Open(f"{folder}/WW_Yield-avg_1980-2010_208.tif").ReadAsArray()
    # ras2 = gdal.Open(out_pth)
    # arr2 = ras2.ReadAsArray()
    # gt = ras2.GetGeoTransform()
    # pr = ras2.GetProjection()
    # diff_arr = calculate_difference_between_rasters(arr1, arr2, no_data_value = -9999, percentage_change=True)
    
    # out_pth = f"{folder}/yield_diff-35_65-80_10-{descr}.tif"
    # write_array_to_raster(diff_arr,
    #                 out_path=out_pth,
    #                 gt=gt,
    #                 pr=pr,
    #                 no_data_value=-9999,
    #                 type_code=None,
    #                 options=['COMPRESS=DEFLATE', 'PREDICTOR=1'],
    #                 driver='GTiff')

    # title = f"yield_diff-35_65-80_10-{descr}"
    # out_pth = f"/beegfs/jaenicke/klimertrag/figures/08_testing_difference_to_anna/difference/{title}.png"
    # plot_difference_raster(diff_arr, -9999, title, out_pth, -25, 25)

    # #####################################################################
    # ## Klimertrag parameters + fertilization + fixed sowing & harvest at one date + vernalization fix on + klima: dwd_core_ensemble + co2 = empty + 10 years forerun
    # descr = 'klim_fert_sfix1_hfix1_vernon_dwdcore_co2empty_10years'
    # file_lst = [
    #     f"{folder}/WW_Yield-avg_2035-2065_205.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_206.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_207.tif"
    # ]
    # out_pth = f"{folder}/WW_Yield-avg_2035-2036_{descr}.tif"
    # calculate_avg_from_list(file_lst, out_pth)

    # arr1 = gdal.Open(f"{folder}/WW_Yield-avg_1980-2010_204.tif").ReadAsArray()
    # ras2 = gdal.Open(out_pth)
    # arr2 = ras2.ReadAsArray()
    # gt = ras2.GetGeoTransform()
    # pr = ras2.GetProjection()
    # diff_arr = calculate_difference_between_rasters(arr1, arr2, no_data_value = -9999, percentage_change=True)
    
    # out_pth = f"{folder}/yield_diff-35_65-80_10-{descr}.tif"
    # write_array_to_raster(diff_arr,
    #                 out_path=out_pth,
    #                 gt=gt,
    #                 pr=pr,
    #                 no_data_value=-9999,
    #                 type_code=None,
    #                 options=['COMPRESS=DEFLATE', 'PREDICTOR=1'],
    #                 driver='GTiff')

    # title = f"yield_diff-35_65-80_10-{descr}"
    # out_pth = f"/beegfs/jaenicke/klimertrag/figures/08_testing_difference_to_anna/difference/{title}.png"
    # plot_difference_raster(diff_arr, -9999, title, out_pth, -25, 25)

    # #####################################################################
    # ## Sustag to Klimertrag carbon allocation
    # ## Sustag parameters + fertilization + fixed sowing & harvest at one date + vernalization fix off + klima: dwd_core_ensemble + co2 = empty + 10 years forerun
    # descr = 'sust_to_klim_vernoff_carb_alloc'
    # file_lst = [
    #     f"{folder}/WW_Yield-avg_2035-2065_222.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_223.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_224.tif"
    # ]
    # out_pth = f"{folder}/WW_Yield-avg_2035-2036_{descr}.tif"
    # calculate_avg_from_list(file_lst, out_pth)

    # arr1 = gdal.Open(f"{folder}/WW_Yield-avg_1980-2010_221.tif").ReadAsArray()
    # ras2 = gdal.Open(out_pth)
    # arr2 = ras2.ReadAsArray()
    # gt = ras2.GetGeoTransform()
    # pr = ras2.GetProjection()
    # diff_arr = calculate_difference_between_rasters(arr1, arr2, no_data_value = -9999, percentage_change=True)
    
    # out_pth = f"{folder}/yield_diff-35_65-80_10-{descr}.tif"
    # write_array_to_raster(diff_arr,
    #                 out_path=out_pth,
    #                 gt=gt,
    #                 pr=pr,
    #                 no_data_value=-9999,
    #                 type_code=None,
    #                 options=['COMPRESS=DEFLATE', 'PREDICTOR=1'],
    #                 driver='GTiff')

    # title = f"yield_diff-35_65-80_10-{descr}"
    # out_pth = f"/beegfs/jaenicke/klimertrag/figures/08_testing_difference_to_anna/difference/{title}.png"
    # plot_difference_raster(diff_arr, -9999, title, out_pth, -25, 25)

    # #####################################################################
    # ## Sustag to Klimertrag specific leaf area
    # ## Sustag parameters + fertilization + fixed sowing & harvest at one date + vernalization fix off + klima: dwd_core_ensemble + co2 = empty + 10 years forerun
    # descr = 'sust_to_klim_vernoff_sla'
    # file_lst = [
    #     f"{folder}/WW_Yield-avg_2035-2065_226.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_227.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_228.tif"
    # ]
    # out_pth = f"{folder}/WW_Yield-avg_2035-2036_{descr}.tif"
    # calculate_avg_from_list(file_lst, out_pth)

    # arr1 = gdal.Open(f"{folder}/WW_Yield-avg_1980-2010_225.tif").ReadAsArray()
    # ras2 = gdal.Open(out_pth)
    # arr2 = ras2.ReadAsArray()
    # gt = ras2.GetGeoTransform()
    # pr = ras2.GetProjection()
    # diff_arr = calculate_difference_between_rasters(arr1, arr2, no_data_value = -9999, percentage_change=True)
    
    # out_pth = f"{folder}/yield_diff-35_65-80_10-{descr}.tif"
    # write_array_to_raster(diff_arr,
    #                 out_path=out_pth,
    #                 gt=gt,
    #                 pr=pr,
    #                 no_data_value=-9999,
    #                 type_code=None,
    #                 options=['COMPRESS=DEFLATE', 'PREDICTOR=1'],
    #                 driver='GTiff')

    # title = f"yield_diff-35_65-80_10-{descr}"
    # out_pth = f"/beegfs/jaenicke/klimertrag/figures/08_testing_difference_to_anna/difference/{title}.png"
    # plot_difference_raster(diff_arr, -9999, title, out_pth, -25, 25)

    # #####################################################################
    # ## Sustag to Klimertrag carbon allocation + specific leaf area
    # ## Sustag parameters + fertilization + fixed sowing & harvest at one date + vernalization fix off + klima: dwd_core_ensemble + co2 = empty + 10 years forerun
    # descr = 'sust_to_klim_vernoff_carb_alloc_sla'
    # file_lst = [
    #     f"{folder}/WW_Yield-avg_2035-2065_230.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_231.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_232.tif"
    # ]
    # out_pth = f"{folder}/WW_Yield-avg_2035-2036_{descr}.tif"
    # calculate_avg_from_list(file_lst, out_pth)

    # arr1 = gdal.Open(f"{folder}/WW_Yield-avg_1980-2010_229.tif").ReadAsArray()
    # ras2 = gdal.Open(out_pth)
    # arr2 = ras2.ReadAsArray()
    # gt = ras2.GetGeoTransform()
    # pr = ras2.GetProjection()
    # diff_arr = calculate_difference_between_rasters(arr1, arr2, no_data_value = -9999, percentage_change=True)
    
    # out_pth = f"{folder}/yield_diff-35_65-80_10-{descr}.tif"
    # write_array_to_raster(diff_arr,
    #                 out_path=out_pth,
    #                 gt=gt,
    #                 pr=pr,
    #                 no_data_value=-9999,
    #                 type_code=None,
    #                 options=['COMPRESS=DEFLATE', 'PREDICTOR=1'],
    #                 driver='GTiff')

    # title = f"yield_diff-35_65-80_10-{descr}"
    # out_pth = f"/beegfs/jaenicke/klimertrag/figures/08_testing_difference_to_anna/difference/{title}.png"
    # plot_difference_raster(diff_arr, -9999, title, out_pth, -25, 25)

    #   #####################################################################
    # ## Sustag to Klimertrag carbon allocation
    # ## Sustag parameters + fertilization + fixed sowing & harvest at one date + vernalization fix on + klima: dwd_core_ensemble + co2 = empty + 10 years forerun
    # descr = 'sust_to_klim_vernon_carb_alloc'
    # file_lst = [
    #     f"{folder}/WW_Yield-avg_2035-2065_254.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_255.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_256.tif"
    # ]
    # out_pth = f"{folder}/WW_Yield-avg_2035-2036_{descr}.tif"
    # calculate_avg_from_list(file_lst, out_pth)

    # arr1 = gdal.Open(f"{folder}/WW_Yield-avg_1980-2010_253.tif").ReadAsArray()
    # ras2 = gdal.Open(out_pth)
    # arr2 = ras2.ReadAsArray()
    # gt = ras2.GetGeoTransform()
    # pr = ras2.GetProjection()
    # diff_arr = calculate_difference_between_rasters(arr1, arr2, no_data_value = -9999, percentage_change=True)
    
    # out_pth = f"{folder}/yield_diff-35_65-80_10-{descr}.tif"
    # write_array_to_raster(diff_arr,
    #                 out_path=out_pth,
    #                 gt=gt,
    #                 pr=pr,
    #                 no_data_value=-9999,
    #                 type_code=None,
    #                 options=['COMPRESS=DEFLATE', 'PREDICTOR=1'],
    #                 driver='GTiff')

    # title = f"yield_diff-35_65-80_10-{descr}"
    # out_pth = f"/beegfs/jaenicke/klimertrag/figures/08_testing_difference_to_anna/difference/{title}.png"
    # plot_difference_raster(diff_arr, -9999, title, out_pth, -25, 25)

    # #####################################################################
    # ## Sustag to Klimertrag specific leaf area
    # ## Sustag parameters + fertilization + fixed sowing & harvest at one date + vernalization fix on + klima: dwd_core_ensemble + co2 = empty + 10 years forerun
    # descr = 'sust_to_klim_vernon_sla'
    # file_lst = [
    #     f"{folder}/WW_Yield-avg_2035-2065_258.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_259.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_260.tif"
    # ]
    # out_pth = f"{folder}/WW_Yield-avg_2035-2036_{descr}.tif"
    # calculate_avg_from_list(file_lst, out_pth)

    # arr1 = gdal.Open(f"{folder}/WW_Yield-avg_1980-2010_257.tif").ReadAsArray()
    # ras2 = gdal.Open(out_pth)
    # arr2 = ras2.ReadAsArray()
    # gt = ras2.GetGeoTransform()
    # pr = ras2.GetProjection()
    # diff_arr = calculate_difference_between_rasters(arr1, arr2, no_data_value = -9999, percentage_change=True)
    
    # out_pth = f"{folder}/yield_diff-35_65-80_10-{descr}.tif"
    # write_array_to_raster(diff_arr,
    #                 out_path=out_pth,
    #                 gt=gt,
    #                 pr=pr,
    #                 no_data_value=-9999,
    #                 type_code=None,
    #                 options=['COMPRESS=DEFLATE', 'PREDICTOR=1'],
    #                 driver='GTiff')

    # title = f"yield_diff-35_65-80_10-{descr}"
    # out_pth = f"/beegfs/jaenicke/klimertrag/figures/08_testing_difference_to_anna/difference/{title}.png"
    # plot_difference_raster(diff_arr, -9999, title, out_pth, -25, 25)

    # #####################################################################
    # ## Sustag to Klimertrag carbon allocation + specific leaf area
    # ## Sustag parameters + fertilization + fixed sowing & harvest at one date + vernalization fix on + klima: dwd_core_ensemble + co2 = empty + 10 years forerun
    # descr = 'sust_vernon_to_klim_carb_alloc_sla'
    # file_lst = [
    #     f"{folder}/WW_Yield-avg_2035-2065_262.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_263.tif",
    #     f"{folder}/WW_Yield-avg_2035-2065_264.tif"
    # ]
    # out_pth = f"{folder}/WW_Yield-avg_2035-2036_{descr}.tif"
    # calculate_avg_from_list(file_lst, out_pth)

    # arr1 = gdal.Open(f"{folder}/WW_Yield-avg_1980-2010_261.tif").ReadAsArray()
    # ras2 = gdal.Open(out_pth)
    # arr2 = ras2.ReadAsArray()
    # gt = ras2.GetGeoTransform()
    # pr = ras2.GetProjection()
    # diff_arr = calculate_difference_between_rasters(arr1, arr2, no_data_value = -9999, percentage_change=True)
    
    # out_pth = f"{folder}/yield_diff-35_65-80_10-{descr}.tif"
    # write_array_to_raster(diff_arr,
    #                 out_path=out_pth,
    #                 gt=gt,
    #                 pr=pr,
    #                 no_data_value=-9999,
    #                 type_code=None,
    #                 options=['COMPRESS=DEFLATE', 'PREDICTOR=1'],
    #                 driver='GTiff')

    # title = f"yield_diff-35_65-80_10-{descr}"
    # out_pth = f"/beegfs/jaenicke/klimertrag/figures/08_testing_difference_to_anna/difference/{title}.png"
    # plot_difference_raster(diff_arr, -9999, title, out_pth, -25, 25)

def main_wb():
    # ##############################################################################
    # ##############################################################################
    # # Winter barley testing with Ehsan


    # #### Compare yields of winter barley with automatic sowing and fixed sowing dates
    # ## Aggregate yields with automatic sowing
    # file_lst = glob.glob("/beegfs/jaenicke/klimertrag/raster/sim-yields/field_condition_modifier_calibration/WB/81/*Yield*.asc")
    # out_pth1 = r"/beegfs/jaenicke/klimertrag/raster/sim-yields/testing_with_ehsan/WB_Yield_avg-orig_params-aut_sowing.tif"
    # # calculate_avg_from_list(file_lst, out_pth1)

    # # ## Aggregate yields with fixed sowing
    # file_lst = glob.glob("/beegfs/jaenicke/klimertrag/raster/sim-yields/testing_with_ehsan/150/*Yield*.asc")
    # out_pth2 = r"/beegfs/jaenicke/klimertrag/raster/sim-yields/testing_with_ehsan/WB_Yield_avg-orig_params-fix_sowing.tif"
    # # calculate_avg_from_list(file_lst, out_pth2)

    # arr1 = gdal.Open(out_pth1).ReadAsArray()
    # ras2 = gdal.Open(out_pth2)
    # arr2 = ras2.ReadAsArray()
    # gt = ras2.GetGeoTransform()
    # pr = ras2.GetProjection()
    # diff_arr = calculate_difference_between_rasters(arr1, arr2, no_data_value = -9999, percentage_change=False)

    # out_pth = f"/beegfs/jaenicke/klimertrag/raster/sim-yields/testing_with_ehsan/WB_yield_diff-fixed-autom.tif"
    # write_array_to_raster(diff_arr,
    #                 out_path=out_pth,
    #                 gt=gt,
    #                 pr=pr,
    #                 no_data_value=-9999,
    #                 type_code=None,
    #                 options=['COMPRESS=DEFLATE', 'PREDICTOR=1'],
    #                 driver='GTiff')

    # title = "yield_diff_fixed-autom"
    # out_pth = f"/beegfs/jaenicke/klimertrag/figures/07_testing_with_ehsan/{title}.png"
    # plot_difference_raster(diff_arr, -9999, title, out_pth, -500, 500)

    #### Compare yields of winter barley with ...
    ## Aggregate yields 
    file_lst = glob.glob("/beegfs/jaenicke/klimertrag/raster/sim-yields/testing_with_ehsan/284/*Yield*.asc")
    out_pth1 = r"/beegfs/jaenicke/klimertrag/raster/sim-yields/testing_with_ehsan/WB_Yield_avg-orig_params-aut_sowing.tif"
    # calculate_avg_from_list(file_lst, out_pth1)

    # ## Aggregate yields with fixed sowing
    file_lst = glob.glob("/beegfs/jaenicke/klimertrag/raster/sim-yields/testing_with_ehsan/150/*Yield*.asc")
    out_pth2 = r"/beegfs/jaenicke/klimertrag/raster/sim-yields/testing_with_ehsan/WB_Yield_avg-orig_params-fix_sowing.tif"
    # calculate_avg_from_list(file_lst, out_pth2)

    arr1 = gdal.Open(out_pth1).ReadAsArray()
    ras2 = gdal.Open(out_pth2)
    arr2 = ras2.ReadAsArray()
    gt = ras2.GetGeoTransform()
    pr = ras2.GetProjection()
    diff_arr = calculate_difference_between_rasters(arr1, arr2, no_data_value = -9999, percentage_change=False)

    out_pth = f"/beegfs/jaenicke/klimertrag/raster/sim-yields/testing_with_ehsan/WB_yield_diff-fixed-autom.tif"
    write_array_to_raster(diff_arr,
                    out_path=out_pth,
                    gt=gt,
                    pr=pr,
                    no_data_value=-9999,
                    type_code=None,
                    options=['COMPRESS=DEFLATE', 'PREDICTOR=1'],
                    driver='GTiff')

    title = "yield_diff_fixed-autom"
    out_pth = f"/beegfs/jaenicke/klimertrag/figures/07_testing_with_ehsan/{title}.png"
    plot_difference_raster(diff_arr, -9999, title, out_pth, -500, 500)

if __name__ == '__main__':
    main()
    # main_wb()