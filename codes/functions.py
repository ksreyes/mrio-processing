import numpy as np

def subset(matrix, row=None, col=None, N=35):

    if len(matrix.shape) == 1:
        n = matrix.shape[0]
        ix = np.arange(0, n)
        if row is not None and row != 0:
            ix = np.arange((abs(row)-1) * N, abs(row) * N)
            if row < 0:
                ix = np.setdiff1d(np.arange(n), ix)
        return matrix[ix]

    else:
        nrow = matrix.shape[0]
        ncol = matrix.shape[1]

        Nrow = N
        Ncol = N
        if nrow % N != 0:
            Nrow = 1
        if ncol % N != 0:
            Ncol = 1

        rowix = np.arange(0, nrow)
        colix = np.arange(0, ncol)

        if row is not None and row != 0:
            rowix = np.arange((abs(row)-1) * Nrow, abs(row) * Nrow)
            if row < 0:
                rowix = np.setdiff1d(np.arange(nrow), rowix)

        if col is not None and col != 0:
            colix = np.arange((abs(col)-1) * Ncol, abs(col) * Ncol)
            if col < 0:
                colix = np.setdiff1d(np.arange(ncol), colix)
        
        return matrix[np.ix_(rowix, colix)]

def ind(s, i=None, G=73, N=35):
    '''
    Selects the indices of country s in any GxN array. Optional argument i selects a specific element in 
    the array!
    '''

    ind_s = list(range((abs(s) - 1) * N, abs(s) * N))
    if s<0:
        ind_s = np.setdiff1d(np.arange(G*N), ind_s)

    if i is not None:
        return ind_s[i]
    else:
        return ind_s

def asvector(matrix):
    ''' 
    Flattens a matrix into a column vector by taking the first column, then appending the second column,
    and so on.
    '''
    vector = np.reshape(matrix, (-1, ), order='F')
    return vector

def zeroout(matrix, row=None, col=None, inverse=False, N=35):
    '''
    Zeroes out the NxN block diagonals of a GNxGN matrix. If inverse=True, all elements but the NxN block
    diagonals are zeroed out. An arbitrary NxN block can be zeroed out by passing ind() in the row and col 
    arguments. 
    '''
    nrow = matrix.shape[0]
    ncol = matrix.shape[1]
    G = max(nrow, ncol) // N

    Nrow = N
    Ncol = N
    if nrow % N != 0:
        Nrow = 1
    if ncol % N != 0:
        Ncol = 1
    
    zeroed_matrix = np.copy(matrix)
    
    if row is None and col is None:
        for k in range(G):
            zeroed_matrix[(k*Nrow):(k*Nrow)+Nrow, (k*Ncol):(k*Ncol)+Ncol] = 0
    else:
        rowix = np.arange((abs(row)-1) * Nrow, abs(row) * Nrow)
        colix = np.arange((abs(col)-1) * Ncol, abs(col) * Ncol)
        if row < 0:
            rowix = np.setdiff1d(np.arange(nrow), rowix)
        if col < 0:
            colix = np.setdiff1d(np.arange(ncol), colix)
        zeroed_matrix[np.ix_(rowix, colix)] = 0
    
    if inverse:
        return matrix - zeroed_matrix
    else:
        return zeroed_matrix

def diagvec(vector, N=35):
    '''
    Splits a vector into N-sized segments and arranges them in a block diagonal matrix. 
    '''
    vector = np.squeeze(vector)
    length = vector.shape[0]
    G = length // N
    matrix = np.zeros((length, G), dtype=np.float32)
    for k in range(G):
        matrix[k*N:(k+1)*N, k] = vector[k*N:(k+1)*N]
    return matrix

def diagmat(matrix, offd=False, N=35):
    '''
    Reshapes a matrix into an appropriate block diagonal matrix. 
    '''
    nrow = matrix.shape[0]
    G = nrow // N
    
    if offd:
        for k in range(G):
            matrix[k*N:(k+1)*N, k] = 0
        vector = np.sum(matrix, axis=1)
    else:
        vector = []
        for k in range(G):
            vector.extend(matrix[k*N:(k+1)*N, k]) 
        vector = np.array(vector)
    
    return diagvec(vector)

def diagrow(vector, N=35):
    '''
    Splits a vector into equal segments of length N, diagonalizes each segment, then stacks the diagonal matrices
    horizontally
    '''
    vector = np.squeeze(vector)
    G = vector.shape[0] // N
    matrix = np.empty((N,0))
    for k in range(G):
        matrix = np.hstack((matrix, np.diag(vector[k*N:(k+1)*N])))
    return matrix

def get_fatdiag(matrix, N=35):
    '''
    Get the N-length vectors running along the diagonal of a matrix.
    '''
    G = matrix.shape[0] // N
    vector = []
    for k in range(G):
        vector.extend(matrix[k*N:(k+1)*N, k])
    return np.array(vector)