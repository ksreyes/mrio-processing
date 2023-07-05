
# Custom functions

The file `functions.py` contains custom functions that aid in the matrix operations performed in the notebooks. The following describes these functions and presents a visual illustration of what they do.

## ind

`ind(s, i=None, G=73, N=35)`

Selects the indices of country $s$ in any $G \times N$ array. Optional argument `i` selects a specific element in the array. For example, with three countries $C$, $J$, and $U$ and two sectors 1 and 2, selecting the block of the $Z$ matrix that corresponds to intermediate sales from country 1 to country 2 can be done as follows:

$$
\begin{alignat*}{3}
  & \texttt{Z[ind(1), ind(2)]} \qquad & & \begin{bmatrix}
        z_{(C1,C1)} & z_{(C1,C2)} & \textcolor{blue}{z_{(C1,J1)}} & \textcolor{blue}{z_{(C1,J2)}} & z_{(C1,U1)} & z_{(C1,U2)} \\
        z_{(C2,C1)} & z_{(C2,C2)} & \textcolor{blue}{z_{(C2,J1)}} & \textcolor{blue}{z_{(C2,J2)}} & z_{(C2,U1)} & z_{(C2,U2)} \\
        z_{(J1,C1)} & z_{(J1,C2)} & z_{(J1,J1)} & z_{(J1,J2)} & z_{(J1,U1)} & z_{(J1,U2)} \\
        z_{(J2,C1)} & z_{(J2,C2)} & z_{(J2,J1)} & z_{(J2,J2)} & z_{(J2,U1)} & z_{(J2,U2)} \\
        z_{(U1,C1)} & z_{(U1,C2)} & z_{(U1,J1)} & z_{(U1,J2)} & z_{(U1,U1)} & z_{(U1,U2)} \\
        z_{(U2,C1)} & z_{(U2,C2)} & z_{(U2,J1)} & z_{(U2,J2)} & z_{(U2,U1)} & z_{(U2,U2)} \\
  \end{bmatrix} \\
\end{alignat*}
$$

Providing a negative index selects all countries except the specified one.

$$
\begin{alignat*}{3}
  & \texttt{Z[ind(-1), ind(2)]} \qquad & & \begin{bmatrix}
        z_{(C1,C1)} & z_{(C1,C2)} & z_{(C1,J1)} & z_{(C1,J2)} & z_{(C1,U1)} & z_{(C1,U2)} \\
        z_{(C2,C1)} & z_{(C2,C2)} & z_{(C2,J1)} & z_{(C2,J2)} & z_{(C2,U1)} & z_{(C2,U2)} \\
        z_{(J1,C1)} & z_{(J1,C2)} & \textcolor{blue}{z_{(J1,J1)}} & \textcolor{blue}{z_{(J1,J2)}} & z_{(J1,U1)} & z_{(J1,U2)} \\
        z_{(J2,C1)} & z_{(J2,C2)} & \textcolor{blue}{z_{(J2,J1)}} & \textcolor{blue}{z_{(J2,J2)}} & z_{(J2,U1)} & z_{(J2,U2)} \\
        z_{(U1,C1)} & z_{(U1,C2)} & \textcolor{blue}{z_{(U1,J1)}} & \textcolor{blue}{z_{(U1,J2)}} & z_{(U1,U1)} & z_{(U1,U2)} \\
        z_{(U2,C1)} & z_{(U2,C2)} & \textcolor{blue}{z_{(U2,J1)}} & \textcolor{blue}{z_{(U2,J2)}} & z_{(U2,U1)} & z_{(U2,U2)} \\
  \end{bmatrix} \\
\end{alignat*}
$$

Note that this uses MRIO indexing rather than Python's 0-based indexing. The first country is called with 1 instead of 0.

## asvector

`asvector(matrix)`

Flattens a matrix into a column vector. This uses the so-called Fortran order, meaning it first goes down a column before moving to the next column. 

$$
\begin{equation*}
    \begin{bmatrix}
      1 & 3 \\
      2 & 4 \\
    \end{bmatrix} \rightarrow
    \begin{bmatrix}
      1 \\
      2 \\
      3 \\
      4 \\
    \end{bmatrix}
  \end{equation*}
$$

## zeroout

`zeroout(matrix, row=None, col=None, inverse=False, N=35)`

Zeroes out any arbitrary block in a matrix. Under default arguments, the function zeroes out the block diagonals. If `inverse=True`, everything but the block diagonals is zeroed out. Specifying `row` and `col` lets one zero out a specific block of the matrix.

$$
\begin{alignat*}{3}
  & \texttt{zeroout(.)} \qquad & & \begin{bmatrix}
    0 & 0 & \times & \times & \times & \times \\
    0 & 0 & \times & \times & \times & \times \\
    \times & \times & 0 & 0 & \times & \times \\
    \times & \times & 0 & 0 & \times & \times \\
    \times & \times & \times & \times & 0 & 0 \\
    \times & \times & \times & \times & 0 & 0 \\
  \end{bmatrix} \\ 
  \\
  & \texttt{zeroout(., inverse=True)} \qquad & & \begin{bmatrix}
    \times & \times & 0 & 0 & 0 & 0 \\
    \times & \times & 0 & 0 & 0 & 0 \\
    0 & 0 & \times & \times & 0 & 0 \\
    0 & 0 & \times & \times & 0 & 0\\
    0 & 0 & 0 & 0 & \times & \times \\
    0 & 0 & 0 & 0 & \times & \times \\
  \end{bmatrix} \\ 
  \\
  & \texttt{zeroout(., row=[0,1], col=[2,3])} \qquad & & \begin{bmatrix}
    \times & \times & 0 & 0 & \times & \times \\
    \times & \times & 0 & 0 & \times & \times \\
    \times & \times & \times & \times & \times & \times \\
    \times & \times & \times & \times & \times & \times \\
    \times & \times & \times & \times & \times & \times \\
    \times & \times & \times & \times & \times & \times \\
  \end{bmatrix}
\end{alignat*}
$$

## diagvec

`diagvec(vector, N=35)`

Splits a vector into $N$-sized segments and arranges them in a block diagonal matrix. 

$$
\begin{equation*}
    \begin{bmatrix}
      \times \\
      \times \\
      \times \\
      \times \\
    \end{bmatrix} \rightarrow
    \begin{bmatrix}
      \times & 0 \\
      \times & 0 \\
      0 & \times \\
      0 & \times \\
    \end{bmatrix}
  \end{equation*}
  $$

## diagmat

`diagmat(matrix, offd=False, N=35)`

Creates a block diagonal matrix from the original matrix, populated either by its block diagonal elements or by its off-block diagonal elements.

$$
\begin{alignat*}{3}
    & \texttt{diagmat(., offd=False)} \qquad & & \begin{bmatrix}
      \times & \bigcirc & \bigtriangleup \\
      \times & \bigcirc & \bigtriangleup \\
      \times & \bigcirc & \bigtriangleup \\
      \times & \bigcirc & \bigtriangleup \\
      \times & \bigcirc & \bigtriangleup \\
      \times & \bigcirc & \bigtriangleup \\
    \end{bmatrix} \rightarrow 
    \begin{bmatrix}
      \times & 0 & 0 \\
      \times & 0 & 0 \\
      0 & \bigcirc & 0 \\
      0 & \bigcirc & 0 \\
      0 & 0 & \bigtriangleup \\
      0 & 0 & \bigtriangleup \\
    \end{bmatrix} \\
    \\
    & \texttt{diagmat(., offd=True)} \qquad & & \begin{bmatrix}
      \times & \bigcirc & \bigtriangleup \\
      \times & \bigcirc & \bigtriangleup \\
      \times & \bigcirc & \bigtriangleup \\
      \times & \bigcirc & \bigtriangleup \\
      \times & \bigcirc & \bigtriangleup \\
      \times & \bigcirc & \bigtriangleup \\
    \end{bmatrix} \rightarrow
    \begin{bmatrix}
      \bigcirc + \bigtriangleup & 0 & 0 \\
      \bigcirc + \bigtriangleup & 0 & 0 \\
      0 & \times + \bigtriangleup & 0 \\
      0 & \times + \bigtriangleup & 0 \\
      0 & 0 & \times + \bigcirc \\
      0 & 0 & \times + \bigcirc \\
    \end{bmatrix}
  \end{alignat*}
$$

## diagrow

`diagrow(vector, N=35)`

Splits a vector into equal segments of length $N$, diagonalizes each segment, then stacks the diagonal matrices horizontally.

$$
\begin{equation*}
    \begin{bmatrix}
      1 \\
      2 \\
      3 \\
      4 \\
    \end{bmatrix} \rightarrow
    \begin{bmatrix}
      1 & 0 & 3 & 0 \\
      0 & 2 & 0 & 4 \\
    \end{bmatrix}
  \end{equation*}
$$