import numpy as np

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
    G = matrix.shape[0] // N
    Ncol = matrix.shape[1] // G
    zeroed_matrix = np.copy(matrix)
    
    if row is None and col is None:
        for k in range(G):
            zeroed_matrix[(k*N):(k*N)+N, (k*Ncol):(k*Ncol)+Ncol] = 0
    else:
        zeroed_matrix[np.ix_(row, col)] = 0
    
    if inverse:
        return matrix - zeroed_matrix
    else:
        return zeroed_matrix

def diagvec(vector, N=35):
    '''
    Splits a vector into N-sized segments and arranges them in a block diagonal matrix. 
    '''
    length = vector.shape[0]
    G = length // N
    matrix = np.zeros((length, G))
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
    G = vector.shape[0] // N
    matrix = np.empty((N,0))
    for k in range(G):
        matrix = np.hstack((matrix, np.diag(vector[k*N:(k+1)*N])))
    return matrix