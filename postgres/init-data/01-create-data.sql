-- This script runs automatically when the container boots!

CREATE TABLE departments (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL
);

CREATE TABLE employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    department_id INT REFERENCES departments(id),
    salary DECIMAL(10, 2),
    hire_date DATE
);

INSERT INTO departments (name) VALUES 
('Sales'), 
('Engineering'), 
('Human Resources');

INSERT INTO employees (name, department_id, salary, hire_date) VALUES 
('Alice Johnson', 2, 85000.00, '2021-03-15'),
('Bob Smith', 1, 62000.00, '2022-01-10'),
('Charlie Davis', 2, 95000.00, '2020-11-01'),
('Diana Prince', 3, 75000.00, '2023-06-01');