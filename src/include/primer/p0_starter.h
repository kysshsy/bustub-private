//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

namespace bustub {

/*
 * The base class defining a Matrix
 */
template <typename T>
class Matrix {
 protected:
  // TODO(P0): Add implementation
  Matrix(int r, int c) {
    this->rows = r;
    this->cols = c;

    this->linear = new T[r * c];
  }

  // # of rows in the matrix
  int rows;
  // # of Columns in the matrix
  int cols;
  // Flattened array containing the elements of the matrix
  // TODO(P0) : Allocate the array in the constructor. Don't forget to free up
  // the array in the destructor.
  T *linear;

 public:
  // Return the # of rows in the matrix
  virtual int GetRows() = 0;

  // Return the # of columns in the matrix
  virtual int GetColumns() = 0;

  // Return the (i,j)th  matrix element
  virtual T GetElem(int i, int j) = 0;

  // Sets the (i,j)th  matrix element to val
  virtual void SetElem(int i, int j, T val) = 0;

  // Sets the matrix elements based on the array arr
  virtual void MatImport(T *arr) = 0;

  virtual ~Matrix() { delete[] linear; }
};

template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  RowMatrix(int r, int c) : Matrix<T>(r, c) {
    this->data_ = new T *[r];
    for (int i = 0; i < r; i++) {
      this->data_[i] = &(this->linear[i * c]);
    }
  }

  int GetRows() override { return this->rows; }

  int GetColumns() override { return this->cols; }

  T GetElem(int i, int j) override { return data_[i][j]; }

  void SetElem(int i, int j, T val) override { data_[i][j] = val; }

  void MatImport(T *arr) override {
    for (int i = 0; i < this->rows; i++) {
      for (int j = 0; j < this->cols; j++) {
        this->data_[i][j] = arr[i * this->cols + j];
      }
    }
  }

  ~RowMatrix() override { delete[] this->data_; }

 private:
  // 2D array containing the elements of the matrix in row-major format
  // TODO(P0): Allocate the array of row pointers in the constructor. Use these pointers
  // to point to corresponding elements of the 'linear' array.
  // Don't forget to free up the array in the destructor.
  T **data_;
};

template <typename T>
class RowMatrixOperations {
 public:
  // Compute (mat1 + mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> AddMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                   std::unique_ptr<RowMatrix<T>> mat2) {
    if (mat1->GetRows() != mat2->GetRows() || mat1->GetColumns() != mat2->GetColumns()) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }

    int m = mat1->GetRows();
    int n = mat1->GetColumns();
    std::unique_ptr<RowMatrix<T>> ptr = std::make_unique<RowMatrix<T>>(m, n);

    for (int i = 0; i < m; i++) {
      for (int j = 0; j < n; j++) {
        ptr->SetElem(i, j, mat1->GetElem(i, j) + mat2->GetElem(i, j));
      }
    }

    return ptr;
  }

  // Compute matrix multiplication (mat1 * mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> MultiplyMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                        std::unique_ptr<RowMatrix<T>> mat2) {
    int m = mat1->GetRows();
    int n = mat1->GetColumns();

    if (mat2->GetRows() != n || mat2->GetColumns() != m) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }

    std::unique_ptr<RowMatrix<T>> ptr = std::make_unique<RowMatrix<T>>(m, m);
    for (int i = 0; i < m; i++) {
      for (int j = 0; j < m; j++) {
        int sum = 0;
        for (int k = 0; k < n; k++) {
          sum += mat1->GetElem(i, k) * mat2->GetElem(k, j);
        }
        ptr->SetElem(i, j, sum);
      }
    }
    return ptr;
  }

  // Simplified GEMM (general matrix multiply) operation
  // Compute (matA * matB + matC). Return nullptr if dimensions mismatch for input matrices
  static std::unique_ptr<RowMatrix<T>> GemmMatrices(std::unique_ptr<RowMatrix<T>> matA,
                                                    std::unique_ptr<RowMatrix<T>> matB,
                                                    std::unique_ptr<RowMatrix<T>> matC) {
    int m = matA->GetRows();
    int n = matA->GetColumns();

    if (matB->GetRows() != n || matB->GetColumns() != m || matC->GetRows() != m || matC->GetColumns() != m) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }

    std::unique_ptr<RowMatrix<T>> ptr = std::make_unique<RowMatrix<T>>(m, m);
    for (int i = 0; i < m; i++) {
      for (int j = 0; j < m; j++) {
        int sum = matC->GetElem(i, j);
        for (int k = 0; k < n; k++) {
          sum += matA->GetElem(i, k) * matB->GetElem(k, j);
        }
        ptr->SetElem(i, j, sum);
      }
    }
    return ptr;
  }
};
}  // namespace bustub
