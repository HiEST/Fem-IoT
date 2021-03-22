import numpy as np


def pandas_to_raster(rast, vars_id, int n_rows,int n_cols):
    cdef int x
    cdef int y
    cdef int[:] row_cell
    cdef float[:,:] row_vars
    cdef float[:,:,:] raster
    cdef int n_items = len(rast)
    cdef int n_vars = len(vars_id)

    raster = np.zeros((n_rows,n_cols,n_vars), dtype=np.float32)
    row_cell = rast["cell"].values
    row_vars = rast[vars_id].values # Only req vars
    for i in range(n_items):
        x = (n_rows - 1) - (row_cell[i] // n_cols)
        y = row_cell[i] % n_cols
        for v in range(n_vars):
            raster[x,y,v] = row_vars[i,v]

    return np.asarray(raster)
