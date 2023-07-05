def ind(s, i=None, G=G, N=N):
    '''
    Selects the indices of country s in any GxN array. If inverse=True, selects all indices but country s.
    Optional argument i selects a specific element in the array.
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