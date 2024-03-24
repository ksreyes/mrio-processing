import numpy as np
import duckdb
import utils

class MRIO:

    np.seterr(divide='ignore', invalid='ignore')

    def __init__(self, file_path, year, full=False):
        
        years = utils.get_years(f'{file_path}')
        if year not in years:
            raise ValueError('selected year is out of bounds.')
        
        mrio = duckdb.sql(f"SELECT * EXCLUDE(t, si) FROM '{file_path}' WHERE t={year}").df()
        
        self.year = year
        self.data = mrio.values
        self.shape = mrio.shape
        self.N = 35
        self.f = 5
        self.G = int((self.shape[0] - 7) / self.N)
        
        '''Extract MRIO components'''
        
        x = self.data[-1][:(self.G * self.N)]
        Z = self.data[:(self.G * self.N)][:, :(self.G * self.N)]
        Y_big = self.data[:(self.G * self.N)][:, (self.G * self.N):-1]
        Y = Y_big @ np.kron(np.eye(self.G, dtype=bool), np.ones((self.f, 1), dtype=bool))
        va = np.sum(self.data[-7:-1][:, :(self.G * self.N)], axis=0)

        self.x = SubMRIO(x, self.G, self.N)
        self.Z = SubMRIO(Z, self.G, self.N)
        self.Y_big = SubMRIO(Y_big, self.G, self.N)
        self.Y = SubMRIO(Y, self.G, self.N)
        self.va = SubMRIO(va, self.G, self.N)

        if full:

            v = np.where(x != 0, va/x, 0)
            A = Z @ np.diag(np.where(x != 0, 1/x, 0))
            B = np.linalg.inv(np.eye(self.G * self.N, dtype=bool) - A)

            self.v = SubMRIO(v, self.G, self.N)
            self.A = SubMRIO(A, self.G, self.N)
            self.B = SubMRIO(B, self.G, self.N)
    
    '''Numpy wrappers'''

    def I(self, dim):
        return SubMRIO(np.eye(dim, dtype=bool), self.G, self.N)
    
    def i(self, dim):
        return SubMRIO(np.ones(dim, dtype=bool), self.G, self.N)
        
    '''Index generators'''
    
    def country_inds(self, exclude=None):

        if exclude is not None and 1 <= exclude <= self.G:
            return np.setdiff1d(np.arange(1, self.G+1, dtype=np.uint8), exclude)
        if exclude is not None and not (1 <= exclude <= self.G):
            raise ValueError(f"'exclude' must be from {1} to {self.G}.")
        else:
            return np.arange(1, self.G+1, dtype=np.uint8)
    
    def sector_inds(self, agg=35):

        c5_ind = np.array([1, 1, 2, 2, 2, 2, 2, 3, 3, 2, 3, 3, 3, 3, 3, 2, 2, 2, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5], dtype=np.uint8)
        c15_ind = np.array([1, 2, 3, 3, 3, 3, 3, 4, 4, 3, 3, 4, 4, 4, 4, 3, 5, 6, 7, 7, 7, 8, 9, 9, 9, 9, 10, 11, 12, 12, 13, 14, 14, 15, 15], dtype=np.uint8)
        
        if agg == 5:
            return c5_ind
        if agg == 15:
            return c15_ind
        if agg == 35:
            return np.arange(1, self.N+1, dtype=np.uint8)
        else:
            raise ValueError("'agg' must be either 5, 15, or 35.")
        
class SubMRIO(MRIO):
    '''
    Class for performing matrix operations and custom methods on MRIO components. 
    '''
    
    def __init__(self, data, ncountries, nsectors):
        self.data = data
        self.shape = data.shape
        self.dtype = data.dtype
        self.G = ncountries
        self.N = nsectors
    
    '''Ensure that results of matrix operations remain in class'''
    
    def __add__(self, other):
        if isinstance(other, SubMRIO):
            other = other.data
            if self.data.dtype == 'bool' and other.dtype == 'bool':
                self.data.dtype = np.uint8
        return SubMRIO(self.data + other, self.G, self.N)
    
    def __sub__(self, other):
        if isinstance(other, SubMRIO):
            other = other.data
            if self.data.dtype == 'bool' and other.dtype == 'bool':
                self.data.dtype = np.uint8
        return SubMRIO(self.data - other, self.G, self.N)
    
    def __rsub__(self, other):
        if isinstance(other, SubMRIO):
            other = other.data
            if self.data.dtype == 'bool' and other.dtype == 'bool':
                self.data.dtype = np.uint8
        return SubMRIO(other - self.data, self.G, self.N)

    def __mul__(self, other):
        if isinstance(other, SubMRIO):
            other = other.data
        return SubMRIO(self.data * other, self.G, self.N)
    
    def __matmul__(self, other):
        return SubMRIO(self.data @ other.data, self.G, self.N)
    
    def __truediv__(self, other):
        if isinstance(other, SubMRIO):
            other = other.data
            if self.data.dtype == 'bool' and other.dtype == 'bool':
                self.data.dtype = np.uint8
        return SubMRIO(np.where(other != 0, self.data/other, 0), self.G, self.N)
    
    def __rtruediv__(self, other):
        if isinstance(other, SubMRIO):
            other = other.data
            if self.data.dtype == 'bool' and other.dtype == 'bool':
                self.data.dtype = np.uint8
        return SubMRIO(np.where(self.data != 0, other/self.data, 0), self.G, self.N)
    
    '''Wrappers for numpy methods'''

    def diag(self):
        return SubMRIO(np.diag(self.data), self.G, self.N)
    
    def invert(self):
        return SubMRIO(np.linalg.inv(self.data), self.G, self.N)
    
    def kron(self, other):
        return SubMRIO(np.kron(self.data, other.data), self.G, self.N)
    
    def t(self):
        return SubMRIO(np.transpose(self.data), self.G, self.N)
    
    '''Custom methods'''
    
    def col_sum(self, chunk=None):
        '''
        Sums matrix column-wise. If chunk is specified, divides matrix into chunk-length groups
        and sums each.
        '''
        if chunk is None:
            return SubMRIO(np.sum(self.data, axis=0), self.G, self.N)
        else:
            if self.data.shape[0] % chunk != 0:
                raise ValueError('data cannot be divided into equal-sized chunks.')
            aggregator = np.kron(np.eye(self.G, dtype=bool), np.ones((1, chunk), dtype=bool))
            return SubMRIO(aggregator @ self.data, self.G, self.N)
    
    def row_sum(self, chunk=None):
        '''
        Sums matrix row-wise. If chunk is specified, divides matrix into chunk-length groups
        and sums each.
        '''
        if chunk is None:
            return SubMRIO(np.sum(self.data, axis=1), self.G, self.N)
        else:
            if self.data.shape[0] % chunk != 0:
                raise ValueError('data cannot be divided into equal-sized chunks.')
            aggregator = np.kron(np.eye(self.G, dtype=bool), np.ones((chunk, 1), dtype=bool))
            return SubMRIO(self.data @ aggregator, self.G, self.N)

    def subset(self, row=None, col=None):
        ''' 
        Subsets a matrix or vector based on country indices. If row or col is None or 0, 
        returns the entire row or column.
        '''

        if not isinstance(row, list):
            if row is not None:
                row = [row]
            else:
                row = [0]
        if not isinstance(col, list):
            if col is not None:
                col = [col]
            else:
                col = [0]
        
        row_signs = [row_i >= 0 for row_i in row]
        if not all(sign == row_signs[0] for sign in row_signs):
            raise ValueError("Row indexes must be either all positive or all negative.")
        
        col_signs = [col_i >= 0 for col_i in col]
        if not all(sign == col_signs[0] for sign in col_signs):
            raise ValueError("Column indexes must be either all positive or all negative.")

        # If input is a vector

        if len(self.data.shape) == 1:
            n = self.data.shape[0]

            if row_signs[0]: 
                ix = []
                for row_i in row:
                    if row_i != 0:
                        ix_i = np.arange((abs(row_i)-1) * self.N, abs(row_i) * self.N)
                        ix.append(ix_i)
                if ix:
                    ix = np.concatenate(ix)
                else:
                    ix = np.arange(n)

            else: 
                ix = np.arange(n)
                for row_i in row:
                    ix_i = np.arange((abs(row_i)-1) * self.N, abs(row_i) * self.N)
                    ix = np.setdiff1d(ix, ix_i)
            
            return SubMRIO(self.data[ix], self.G, self.N)

        # If input is a matrix

        else:
            nrow = self.data.shape[0]
            ncol = self.data.shape[1]

            Nrow, Ncol = self.N, self.N
            if nrow % self.N != 0:
                Nrow = 1
            if ncol % self.N != 0:
                Ncol = 1

            if row_signs[0]: 
                rowix = []
                for row_i in row:
                    if row_i != 0:
                        rowix_i = np.arange((abs(row_i)-1) * Nrow, abs(row_i) * Nrow)
                        rowix.append(rowix_i)
                if rowix:
                    rowix = np.concatenate(rowix)
                else:
                    rowix = np.arange(nrow)
            else: 
                rowix = np.arange(nrow)
                for row_i in row:
                    rowix_i = np.arange((abs(row_i)-1) * Nrow, abs(row_i) * Nrow)
                    rowix = np.setdiff1d(rowix, rowix_i)
            
            if col_signs[0]: 
                colix = []
                for col_i in col:
                    if col_i != 0:
                        colix_i = np.arange((abs(col_i)-1) * Ncol, abs(col_i) * Ncol)
                        colix.append(colix_i)
                if colix:
                    colix = np.concatenate(colix)
                else:
                    colix = np.arange(ncol)
            else: 
                colix = np.arange(ncol)
                for col_i in col:
                    colix_i = np.arange((abs(col_i)-1) * Ncol, abs(col_i) * Ncol)
                    colix = np.setdiff1d(colix, colix_i)

            return SubMRIO(self.data[np.ix_(rowix, colix)], self.G, self.N)
    
    def asvector(self):
        ''' 
        Flattens a matrix into a column vector by taking the first column, then appending the second column,
        and so on.
        '''
        vector = np.reshape(self.data, (-1, ), order='F')
        return SubMRIO(vector, self.G, self.N)
    
    def zeroout(self, row=None, col=None, inverse=False):
        '''
        Zeroes out the NxN block diagonals of a GNxGN matrix. If inverse=True, all elements but the NxN block
        diagonals are zeroed out. An arbitrary NxN block can be zeroed out by passing indexes in the row and col 
        arguments. 
        '''
        nrow = self.data.shape[0]
        ncol = self.data.shape[1]
        GG = max(nrow, ncol) // self.N

        Nrow, Ncol = self.N, self.N
        if nrow % self.N != 0:
            Nrow = 1
        if ncol % self.N != 0:
            Ncol = 1
        
        zeroed_matrix = np.copy(self.data)
        if row is None and col is None:
            for k in range(GG):
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
            return SubMRIO(self.data - zeroed_matrix, self.G, self.N)
        else:
            return SubMRIO(zeroed_matrix, self.G, self.N)
    
    def get_fatdiag(self):
        '''
        Get the N-length vectors running along the diagonal of a matrix.
        '''
        GG = self.data.shape[0] // self.N
        vector = []
        for k in range(GG):
            vector.extend(self.data[k * self.N:(k+1) * self.N, k])
        return SubMRIO(np.array(vector), self.G, self.N)
    
    def diagvec(self):
        '''
        Splits a vector into N-sized segments and arranges them in a block diagonal matrix. 
        '''
        vector = np.squeeze(self.data)
        length = vector.shape[0]
        GG = length // self.N
        matrix = np.zeros((length, GG))
        for k in range(GG):
            matrix[k * self.N:(k+1) * self.N, k] = vector[k * self.N:(k+1) * self.N]
        return SubMRIO(matrix, self.G, self.N)

    def diagmat(self, offd=False):
        '''
        Reshapes a matrix into an appropriate block diagonal matrix. 
        '''
        matrix = np.copy(self.data)
        nrow = matrix.shape[0]
        GG = nrow // self.N
        
        if offd:
            for k in range(GG):
                matrix[k * self.N:(k+1) * self.N, k] = 0
            vector = np.sum(matrix, axis=1)
        else:
            vector = []
            for k in range(GG):
                vector.extend(matrix[k * self.N:(k+1) * self.N, k]) 
            vector = np.array(vector)
        
        return SubMRIO(vector, self.G, self.N).diagvec()

    def diagrow(self):
        '''
        Splits a vector into equal segments of length N, diagonalizes each segment, then stacks the diagonal matrices
        horizontally
        '''
        vector = np.squeeze(self.data)
        GG = vector.shape[0] // self.N
        matrix = np.empty((self.N, 0))
        for k in range(GG):
            matrix = np.hstack((matrix, np.diag(vector[k * self.N:(k+1) * self.N])))
        return SubMRIO(matrix, self.G, self.N)
