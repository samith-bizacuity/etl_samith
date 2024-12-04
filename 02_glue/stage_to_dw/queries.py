queries = {
    'OFFICES' : [
        """
        UPDATE devdw.Offices AS B
        SET
            city = A.city,
            phone = A.phone,
            addressLine1 = A.addressLine1,
            addressLine2 = A.addressLine2,
            state = A.state,
            country = A.country,
            postalCode = A.postalCode,
            territory = A.territory,
            src_update_timestamp = A.update_timestamp,
            dw_update_timestamp = CURRENT_TIMESTAMP,  
            etl_batch_no = %s,  
            etl_batch_date = %s 
        FROM devstage.Offices AS A
        WHERE A.officeCode = B.officeCode;
        """,
        """
        INSERT INTO devdw.Offices
        (
            officeCode,
            city,
            phone,
            addressLine1,
            addressLine2,
            state,
            country,
            postalCode,
            territory,
            src_create_timestamp,
            src_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT
            A.officeCode,
            A.city,
            A.phone,
            A.addressLine1,
            A.addressLine2,
            A.state,
            A.country,
            A.postalCode,
            A.territory,
            A.create_timestamp,
            A.update_timestamp,
            %s, 
            %s  
        FROM devstage.Offices A
        LEFT JOIN devdw.Offices B ON A.officeCode = B.officeCode
        WHERE B.officeCode IS NULL;
        """
    ],

    'EMPLOYEES': [
        """
        UPDATE devdw.Employees
        SET
            lastName = A.lastName,
            firstName = A.firstName,
            extension = A.extension,
            email = A.email,
            officeCode = A.officeCode,
            reportsTo = A.reportsTo,
            jobTitle = A.jobTitle,
            dw_office_id = O.dw_office_id,
            src_update_timestamp = A.update_timestamp,
            dw_update_timestamp = CURRENT_TIMESTAMP,
            etl_batch_no = %s,  
            etl_batch_date = %s  
        FROM devdw.Employees AS B 
        JOIN devstage.Employees AS A ON A.employeeNumber = B.employeeNumber
        JOIN devdw.Offices AS O ON O.officeCode = A.officeCode
        WHERE A.employeeNumber = B.employeeNumber;
        """,
        """
        INSERT INTO devdw."Employees"
        (
            employeeNumber,
            lastName,
            firstName,
            extension,
            email,
            officeCode,
            reportsTo,
            jobTitle,
            dw_office_id,
            src_create_timestamp,
            src_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT
            A.employeeNumber,
            A.lastName,
            A.firstName,
            A.extension,
            A.email,
            A.officeCode,
            A.reportsTo,
            A.jobTitle,
            O.dw_office_id,
            A.create_timestamp,
            A.update_timestamp,
            %s, 
            %s  
        FROM devstage."Employees" A
        LEFT JOIN devdw."Employees" B ON A.employeeNumber = B.employeeNumber
        JOIN devdw."Offices" O ON A.officeCode = O.officeCode
        WHERE B.employeeNumber IS NULL;
        """,
        """
        UPDATE devdw."Employees" AS dw1
        SET
            dw_reporting_employee_id = dw2.dw_employee_id
        FROM devdw."Employees" AS dw2
        WHERE dw1.reportsTo = dw2.employeeNumber;
        """

    ],

    'CUSTOMERS' : [
        """
        UPDATE devdw.Customers B
        SET 
            contactLastName = A.contactLastName,
            contactFirstName = A.contactFirstName,
            phone = A.phone,
            addressLine1 = A.addressLine1,
            addressLine2 = A.addressLine2,
            city = A.city,
            state = A.state,
            postalCode = A.postalCode,
            country = A.country,
            salesRepEmployeeNumber = A.salesRepEmployeeNumber,
            creditLimit = A.creditLimit,
            dw_sales_employee_id = E.dw_employee_id,
            src_update_timestamp = A.update_timestamp,
            dw_update_timestamp = CURRENT_TIMESTAMP,
            etl_batch_no = %s,
            etl_batch_date = %s
        FROM devstage.Customers A
        LEFT JOIN devdw.Employees E ON A.salesRepEmployeeNumber = E.employeeNumber
        WHERE A.customerNumber = B.src_customerNumber;
        """,
        """
        INSERT INTO devdw.Customers
        (
            src_customerNumber,
            customerName,
            contactLastName,
            contactFirstName,
            phone,
            addressLine1,
            addressLine2,
            city,
            state,
            postalCode,
            country,
            salesRepEmployeeNumber,
            creditLimit,
            dw_sales_employee_id,
            src_create_timestamp,
            src_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT 
            A.customerNumber,
            A.customerName,
            A.contactLastName,
            A.contactFirstName,
            A.phone,
            A.addressLine1,
            A.addressLine2,
            A.city,
            A.state,
            A.postalCode,
            A.country,
            A.salesRepEmployeeNumber,
            A.creditLimit,
            E.dw_employee_id,
            A.create_timestamp,
            A.update_timestamp,
            %s,
            %s
        FROM devstage.Customers A
        LEFT JOIN devdw.Customers B ON A.customerNumber = B.src_customerNumber
        LEFT JOIN devdw.Employees E ON A.salesRepEmployeeNumber = E.employeeNumber
        WHERE B.src_customerNumber IS NULL;
        """
    ],

    'ORDERS' : [
        """
        UPDATE devdw.Orders B
        SET 
            requiredDate = A.requiredDate,
            cancelledDate = A.cancelledDate,
            shippedDate = A.shippedDate,
            status = A.status,
            src_update_timestamp = A.update_timestamp,
            dw_update_timestamp = CURRENT_TIMESTAMP,
            etl_batch_no = %s,  -- for etl_batch_no
            etl_batch_date = %s  -- for etl_batch_date
        FROM devstage.Orders A
        WHERE A.orderNumber = B.src_orderNumber;
        """,
        """
        INSERT INTO devdw.Orders
        (
            dw_customer_id,
            src_orderNumber,
            orderDate,
            requiredDate,
            cancelledDate,
            shippedDate,
            status,
            src_customerNumber,
            src_create_timestamp,
            src_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT 
            C.dw_customer_id,
            A.orderNumber,
            A.orderDate,
            A.requiredDate,
            A.cancelledDate,
            A.shippedDate,
            A.status,
            A.customerNumber,
            A.create_timestamp,
            A.update_timestamp,
            %s,  -- for etl_batch_no
            %s   -- for etl_batch_date
        FROM devstage.Orders A
        LEFT JOIN devdw.Orders B ON A.orderNumber = B.src_orderNumber
        JOIN devdw.Customers C ON A.customerNumber = C.src_customerNumber
        WHERE B.src_orderNumber IS NULL;
        """
    ],

    'ORDERDETAILS' : [
        """
        UPDATE devdw.OrderDetails B
        SET 
            dw_order_id = O.dw_order_id,
            dw_product_id = P.dw_product_id,
            src_orderNumber = A.orderNumber,
            src_productCode = A.productCode,
            quantityOrdered = A.quantityOrdered,
            priceEach = A.priceEach,
            orderLineNumber = A.orderLineNumber,
            src_create_timestamp = A.create_timestamp,
            src_update_timestamp = A.update_timestamp,
            dw_update_timestamp = CURRENT_TIMESTAMP,  -- PostgreSQL doesn't need parentheses
            etl_batch_no = %s,
            etl_batch_date = %s  
        FROM devstage.OrderDetails A
        JOIN devdw.Products P ON A.productCode = P.src_productCode
        JOIN devdw.Orders O ON A.orderNumber = O.src_orderNumber
        WHERE A.orderNumber = B.src_orderNumber
        AND A.productCode = B.src_productCode;
        """,
        """
        INSERT INTO devdw.OrderDetails
        (
        dw_order_id,
        dw_product_id,
        src_orderNumber,
        src_productCode,
        quantityOrdered,
        priceEach,
        orderLineNumber,
        src_create_timestamp,
        src_update_timestamp,
        etl_batch_no,
        etl_batch_date
        )
        SELECT O.dw_order_id,
            P.dw_product_id,
            A.orderNumber,
            A.productCode,
            A.quantityOrdered,
            A.priceEach,
            A.orderLineNumber,
            A.create_timestamp,
            A.update_timestamp,
            %s,  
            %s  
        FROM devstage.OrderDetails A
        LEFT JOIN devdw.OrderDetails B
        ON A.orderNumber = B.src_orderNumber
        AND A.productCode = B.src_productCode
        JOIN devdw.Products P ON A.productCode = P.src_productCode
        JOIN devdw.Orders O ON A.orderNumber = O.src_orderNumber
        WHERE B.src_orderNumber IS NULL;
        """
    ],

    'PRODUCTS' : [
        """
        UPDATE devdw.Products B
        SET 
            productName = A.productName,
            productLine = A.productLine,
            productScale = A.productScale,
            productVendor = A.productVendor,
            quantityInStock = A.quantityInStock,
            buyPrice = A.buyPrice,
            MSRP = A.MSRP,
            dw_product_line_id = PL.dw_product_line_id,
            src_update_timestamp = A.update_timestamp,
            dw_update_timestamp = CURRENT_TIMESTAMP,
            etl_batch_no = %s,  -- for etl_batch_no
            etl_batch_date = %s  -- for etl_batch_date
        FROM devstage.Products A
        JOIN devdw.ProductLines PL ON A.productLine = PL.productLine
        WHERE A.productCode = B.src_productCode;
        """,
        """
        INSERT INTO devdw.Products
        (
            src_productCode,
            productName,
            productLine,
            productScale,
            productVendor,
            quantityInStock,
            buyPrice,
            MSRP,
            dw_product_line_id,
            src_create_timestamp,
            src_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT 
            A.productCode,
            A.productName,
            A.productLine,
            A.productScale,
            A.productVendor,
            A.quantityInStock,
            A.buyPrice,
            A.MSRP,
            PL.dw_product_line_id,
            A.create_timestamp,
            A.update_timestamp,
            %s,  -- for etl_batch_no
            %s   -- for etl_batch_date
        FROM devstage.Products A
        LEFT JOIN devdw.Products B ON A.productCode = B.src_productCode
        JOIN devdw.ProductLines PL ON A.productLine = PL.productLine
        WHERE B.src_productCode IS NULL;
        """
    ],

    'PRODUCTLINES' : [
        """
        UPDATE devdw.ProductLines B
        SET 
            productLine = A.productLine,
            src_update_timestamp = A.update_timestamp,
            dw_update_timestamp = CURRENT_TIMESTAMP,
            etl_batch_no = %s,  -- for etl_batch_no
            etl_batch_date = %s  -- for etl_batch_date
        FROM devstage.ProductLines A
        WHERE A.productLine = B.productLine;
        """,
        """
        INSERT INTO devdw.ProductLines
        (
            productLine,
            src_create_timestamp,
            src_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT 
            A.productLine,
            A.create_timestamp,
            A.update_timestamp,
            %s,  -- for etl_batch_no
            %s   -- for etl_batch_date
        FROM devstage.ProductLines A
        LEFT JOIN devdw.ProductLines B ON A.productLine = B.productLine
        WHERE B.productLine IS NULL;
        """
    ],

    'PAYMENTS' : [
        """
        INSERT INTO devdw.Payments
        (
            dw_customer_id,
            src_customerNumber,
            checkNumber,
            paymentDate,
            amount,
            src_create_timestamp,
            src_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT 
            C.dw_customer_id,
            A.customerNumber,
            A.checkNumber,
            A.paymentDate,
            A.amount,
            A.create_timestamp,
            A.update_timestamp,
            %s,  -- for etl_batch_no
            %s   -- for etl_batch_date
        FROM devstage.Payments A
        JOIN devdw.Customers C ON A.customerNumber = C.src_customerNumber;
        """
    ],

    'CUSTOMER_HISTORY' : [
        """
        UPDATE devdw.customer_history ch
        SET 
            dw_active_record_ind = 0,
            effective_to_date = %s::date - INTERVAL '1 day',  -- for etl_batch_date, adjusted for PostgreSQL
            update_etl_batch_no = %s,  -- for etl_batch_no
            update_etl_batch_date = %s,  -- for etl_batch_date
            dw_update_timestamp = CURRENT_TIMESTAMP
        FROM devdw.Customers C
        WHERE ch.dw_customer_id = C.dw_customer_id
        AND ch.dw_active_record_ind = 1
        AND C.creditLimit <> ch.creditLimit;
        """,
        """
        INSERT INTO devdw.customer_history 
        (
            dw_customer_id,
            creditLimit,
            effective_from_date,
            dw_active_record_ind,
            create_etl_batch_no,
            create_etl_batch_date
        )
        SELECT 
            C.dw_customer_id,
            C.creditLimit,
            %s,  -- for etl_batch_date
            1,  -- for dw_active_record_ind
            %s,  -- for etl_batch_no
            %s   -- for etl_batch_date
        FROM devdw.Customers C
        LEFT JOIN devdw.customer_history ch
            ON C.dw_customer_id = ch.dw_customer_id 
            AND ch.dw_active_record_ind = 1
        WHERE ch.dw_customer_id IS NULL;
        """
    ],

    'PRODUCT_HISTORY' : [
        """
        UPDATE devdw.product_history ph
        SET 
            dw_active_record_ind = 0,
            effective_to_date = %s::date - INTERVAL '1 day',  -- Adjusting date calculation for PostgreSQL
            update_etl_batch_no = %s,  -- for etl_batch_no
            update_etl_batch_date = %s,  -- for etl_batch_date
            dw_update_timestamp = CURRENT_TIMESTAMP
        FROM devdw.Products P
        WHERE ph.dw_product_id = P.dw_product_id
        AND ph.dw_active_record_ind = 1
        AND P.MSRP <> ph.MSRP;
        """,
        """
        INSERT INTO devdw.product_history
        (
            dw_product_id,
            MSRP,
            effective_from_date,
            dw_active_record_ind,
            create_etl_batch_no,
            create_etl_batch_date
        )
        SELECT 
            P.dw_product_id,
            P.MSRP,
            %s,  -- for etl_batch_date
            1,  -- dw_active_record_ind is always 1 for new records
            %s,  -- for etl_batch_no
            %s   -- for etl_batch_date
        FROM devdw.Products P
        LEFT JOIN devdw.product_history ph
            ON P.dw_product_id = ph.dw_product_id 
            AND ph.dw_active_record_ind = 1
        WHERE ph.dw_product_id IS NULL;
        """
    ],

    'DAILY_PRODUCT_SUMMARY' : [
        """
        INSERT INTO devdw.daily_product_summary
        (
            summary_date,
            dw_product_id,
            customer_apd,
            product_cost_amount,
            product_mrp_amount,
            cancelled_product_qty,
            cancelled_cost_amount,
            cancelled_mrp_amount,
            cancelled_order_apd,
            dw_create_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        WITH CTE AS
        (
            -- Orders data (non-cancelled orders)
            SELECT CAST(o.orderDate AS DATE) AS summary_date,
                p.dw_product_id,
                1 AS customer_apd,
                SUM(od.quantityOrdered * od.priceEach) AS product_cost_amount,
                SUM(od.quantityOrdered * p.MSRP) AS product_mrp_amount,
                0 AS cancelled_product_qty,
                0 AS cancelled_cost_amount,
                0 AS cancelled_mrp_amount,
                0 AS cancelled_order_apd
            FROM devdw.Products p
            JOIN devdw.OrderDetails od ON p.dw_product_id = od.dw_product_id
            JOIN devdw.Orders o ON od.dw_order_id = o.dw_order_id
            WHERE o.orderDate >= %s  -- etl_batch_date parameter
            GROUP BY CAST(o.orderDate AS DATE),
                    p.dw_product_id

            UNION ALL

            -- Cancelled orders data
            SELECT CAST(o.cancelledDate AS DATE) AS summary_date,
                p.dw_product_id,
                1 AS customer_apd,
                0 AS product_cost_amount,
                0 AS product_mrp_amount,
                COUNT(DISTINCT o.dw_order_id) AS cancelled_product_qty,
                SUM(od.quantityOrdered * od.priceEach) AS cancelled_cost_amount,
                SUM(od.quantityOrdered * p.MSRP) AS cancelled_mrp_amount,
                1 AS cancelled_order_apd
            FROM devdw.Products p
            JOIN devdw.OrderDetails od ON p.dw_product_id = od.dw_product_id
            JOIN devdw.Orders o ON od.dw_order_id = o.dw_order_id
            WHERE o.cancelledDate >= %s  -- etl_batch_date parameter
            GROUP BY CAST(o.cancelledDate AS DATE),
                    p.dw_product_id
        )
        SELECT summary_date,
            dw_product_id,
            MAX(customer_apd) AS customer_apd,
            MAX(product_cost_amount) AS product_cost_amount,
            MAX(product_mrp_amount) AS product_mrp_amount,
            MAX(cancelled_product_qty) AS cancelled_product_qty,
            MAX(cancelled_cost_amount) AS cancelled_cost_amount,
            MAX(cancelled_mrp_amount) AS cancelled_mrp_amount,
            MAX(cancelled_order_apd) AS cancelled_order_apd,
            CURRENT_TIMESTAMP AS dw_create_timestamp,
            %s AS etl_batch_no,  -- etl_batch_no parameter
            %s AS etl_batch_date  -- etl_batch_date parameter
        FROM CTE
        GROUP BY summary_date,
                dw_product_id;
        """
    ],

    'DAILY_CUSTOMER_SUMMARY' : [
        """
        INSERT INTO devdw.daily_customer_summary
        (
            summary_date,
            dw_customer_id,
            order_count,
            order_apd,
            order_amount,
            order_cost_amount,
            order_mrp_amount,
            products_ordered_qty,
            products_items_qty,
            cancelled_order_count,
            cancelled_order_amount,
            cancelled_order_apd,
            shipped_order_count,
            shipped_order_amount,
            shipped_order_apd,
            payment_apd,
            payment_amount,
            new_customer_apd,
            new_customer_paid_apd,
            create_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        WITH CTE AS
        (
            -- Orders data
            SELECT CAST(o.orderDate AS DATE) AS summary_date,
                o.dw_customer_id,
                COUNT(DISTINCT o.dw_order_id) AS order_count,
                1 AS order_apd,
                SUM(od.priceEach * od.quantityOrdered) AS order_amount,
                SUM(p.buyPrice * od.quantityOrdered) AS order_cost_amount,
                SUM(p.MSRP * od.quantityOrdered) AS order_mrp_amount,
                COUNT(DISTINCT od.src_productCode) AS products_ordered_qty,
                SUM(od.quantityOrdered) AS products_items_qty,
                0 AS cancelled_order_count,
                0 AS cancelled_order_amount,
                0 AS cancelled_order_apd,
                0 AS shipped_order_count,
                0 AS shipped_order_amount,
                0 AS shipped_order_apd,
                0 AS payment_apd,
                0 AS payment_amount,
                0 AS new_customer_apd,
                0 AS new_customer_paid_apd
            FROM devdw.Orders o
            JOIN devdw.OrderDetails od ON o.dw_order_id = od.dw_order_id
            JOIN devdw.Products p ON od.dw_product_id = p.dw_product_id
            WHERE o.orderDate >= %s  -- etl_batch_date parameter
            GROUP BY CAST(o.orderDate AS DATE),
                    o.dw_customer_id

            UNION ALL

            -- Cancelled orders data
            SELECT CAST(o.cancelledDate AS DATE) AS summary_date,
                o.dw_customer_id,
                0 AS order_count,
                0 AS order_apd,
                0 AS order_amount,
                0 AS order_cost_amount,
                0 AS order_mrp_amount,
                0 AS products_ordered_qty,
                0 AS products_items_qty,
                COUNT(DISTINCT o.dw_order_id) AS cancelled_order_count,
                SUM(od.priceEach * od.quantityOrdered) AS cancelled_order_amount,
                1 AS cancelled_order_apd,
                0 AS shipped_order_count,
                0 AS shipped_order_amount,
                0 AS shipped_order_apd,
                0 AS payment_apd,
                0 AS payment_amount,
                0 AS new_customer_apd,
                0 AS new_customer_paid_apd
            FROM devdw.Orders o
            JOIN devdw.OrderDetails od ON o.dw_order_id = od.dw_order_id
            WHERE o.cancelledDate >= %s  -- etl_batch_date parameter
            GROUP BY CAST(o.cancelledDate AS DATE),
                    o.dw_customer_id

            UNION ALL

            -- Shipped orders data
            SELECT CAST(o.shippedDate AS DATE) AS summary_date,
                o.dw_customer_id,
                0 AS order_count,
                0 AS order_apd,
                0 AS order_amount,
                0 AS order_cost_amount,
                0 AS order_mrp_amount,
                0 AS products_ordered_qty,
                0 AS products_items_qty,
                0 AS cancelled_order_count,
                0 AS cancelled_order_amount,
                0 AS cancelled_order_apd,
                COUNT(DISTINCT o.dw_order_id) AS shipped_order_count,
                SUM(od.priceEach * od.quantityOrdered) AS shipped_order_amount,
                1 AS shipped_order_apd,
                0 AS payment_apd,
                0 AS payment_amount,
                0 AS new_customer_apd,
                0 AS new_customer_paid_apd
            FROM devdw.Orders o
            JOIN devdw.OrderDetails od ON o.dw_order_id = od.dw_order_id
            WHERE o.shippedDate >= %s  -- etl_batch_date parameter
            AND o.status = 'Shipped'
            GROUP BY CAST(o.shippedDate AS DATE),
                    o.dw_customer_id

            UNION ALL

            -- Payments data
            SELECT CAST(p.paymentDate AS DATE) AS summary_date,
                p.dw_customer_id,
                0 AS order_count,
                0 AS order_apd,
                0 AS order_amount,
                0 AS order_cost_amount,
                0 AS order_mrp_amount,
                0 AS products_ordered_qty,
                0 AS products_items_qty,
                0 AS cancelled_order_count,
                0 AS cancelled_order_amount,
                0 AS cancelled_order_apd,
                0 AS shipped_order_count,
                0 AS shipped_order_amount,
                0 AS shipped_order_apd,
                1 AS payment_apd,
                SUM(p.amount) AS payment_amount,
                0 AS new_customer_apd,
                0 AS new_customer_paid_apd
            FROM devdw.Payments p
            WHERE p.paymentDate >= %s  -- etl_batch_date parameter
            GROUP BY CAST(p.paymentDate AS DATE),
                    p.dw_customer_id

            UNION ALL

            -- New customer data
            SELECT CAST(c.src_create_timestamp AS DATE) AS summary_date,
                c.dw_customer_id,
                0 AS order_count,
                0 AS order_apd,
                0 AS order_amount,
                0 AS order_cost_amount,
                0 AS order_mrp_amount,
                0 AS products_ordered_qty,
                0 AS products_items_qty,
                0 AS cancelled_order_count,
                0 AS cancelled_order_amount,
                0 AS cancelled_order_apd,
                0 AS shipped_order_count,
                0 AS shipped_order_amount,
                0 AS shipped_order_apd,
                0 AS payment_apd,
                0 AS payment_amount,
                1 AS new_customer_apd,
                0 AS new_customer_paid_apd
            FROM devdw.Customers c
            WHERE c.src_create_timestamp >= %s  -- etl_batch_date parameter
        )
        SELECT summary_date,
            dw_customer_id,
            MAX(order_count) AS order_count,
            MAX(order_apd) AS order_apd,
            MAX(order_amount) AS order_amount,
            MAX(order_cost_amount) AS order_cost_amount,
            MAX(order_mrp_amount) AS order_mrp_amount,
            MAX(products_ordered_qty) AS products_ordered_qty,
            MAX(products_items_qty) AS products_items_qty,
            MAX(cancelled_order_count) AS cancelled_order_count,
            MAX(cancelled_order_amount) AS cancelled_order_amount,
            MAX(cancelled_order_apd) AS cancelled_order_apd,
            MAX(shipped_order_count) AS shipped_order_count,
            MAX(shipped_order_amount) AS shipped_order_amount,
            MAX(shipped_order_apd) AS shipped_order_apd,
            MAX(payment_apd) AS payment_apd,
            MAX(payment_amount) AS payment_amount,
            MAX(new_customer_apd) AS new_customer_apd,
            MAX(new_customer_paid_apd) AS new_customer_paid_apd,
            CURRENT_TIMESTAMP AS create_timestamp,
            %s AS etl_batch_no,  -- etl_batch_no parameter
            %s AS etl_batch_date  -- etl_batch_date parameter
        FROM CTE
        GROUP BY summary_date,
                dw_customer_id;
        """
    ],

    'MONTHLY_PRODUCT_SUMMARY' : [
        """
        WITH CTE AS
(
  SELECT TO_CHAR(dps.summary_date, 'YYYY-MM-01')::DATE AS start_of_the_month_date, -- Date formatting in PostgreSQL
         dps.dw_product_id,
         SUM(dps.customer_apd) AS customer_apd,
         CASE WHEN MAX(dps.customer_apd) > 0 THEN 1 ELSE 0 END AS customer_apm,
         SUM(dps.product_cost_amount) AS product_cost_amount,
         SUM(dps.product_mrp_amount) AS product_mrp_amount,
         SUM(dps.cancelled_product_qty) AS cancelled_product_qty,
         SUM(dps.cancelled_cost_amount) AS cancelled_cost_amount,
         SUM(dps.cancelled_mrp_amount) AS cancelled_mrp_amount,
         SUM(dps.cancelled_order_apd) AS cancelled_order_apd,
         CASE WHEN MAX(dps.cancelled_order_apd) > 0 THEN 1 ELSE 0 END AS cancelled_order_apm
  FROM devdw.daily_product_summary dps
  WHERE dps.summary_date >= %s  -- Replaced with parameter placeholder
  GROUP BY TO_CHAR(dps.summary_date, 'YYYY-MM-01')::DATE, dps.dw_product_id
) 
UPDATE devdw.monthly_product_summary
SET
    customer_apd = mps.customer_apd + c.customer_apd,
    customer_apm = (mps.customer_apm::int | c.customer_apm::int),  -- Bitwise OR, casting booleans to integers
    product_cost_amount = mps.product_cost_amount + c.product_cost_amount,
    product_mrp_amount = mps.product_mrp_amount + c.product_mrp_amount,
    cancelled_product_qty = mps.cancelled_product_qty + c.cancelled_product_qty,
    cancelled_cost_amount = mps.cancelled_cost_amount + c.cancelled_cost_amount,
    cancelled_mrp_amount = mps.cancelled_mrp_amount + c.cancelled_mrp_amount,
    cancelled_order_apd = mps.cancelled_order_apd + c.cancelled_order_apd,
    cancelled_order_apm = (mps.cancelled_order_apm::int | c.cancelled_order_apm::int),  -- Bitwise OR
    dw_update_timestamp = CURRENT_TIMESTAMP,
    etl_batch_no = %s,  -- Replaced with parameter placeholder
    etl_batch_date = %s  -- Replaced with parameter placeholder
FROM CTE c
JOIN devdw.monthly_product_summary mps
ON mps.start_of_the_month_date = c.start_of_the_month_date
  AND mps.dw_product_id = c.dw_product_id;
""",
"""
-- Insert new records into the monthly_product_summary table
INSERT INTO devdw.monthly_product_summary (
   start_of_the_month_date,
   dw_product_id,
   customer_apd,
   customer_apm, 
   product_cost_amount,
   product_mrp_amount,
   cancelled_product_qty,
   cancelled_cost_amount,
   cancelled_mrp_amount,
   cancelled_order_apd,
   cancelled_order_apm,
   etl_batch_no,
   etl_batch_date
)
SELECT
   TO_CHAR(dps.summary_date, 'YYYY-MM-01')::DATE AS start_of_the_month_date, -- Date formatting in PostgreSQL
   dps.dw_product_id, 
   SUM(dps.customer_apd),
   CASE WHEN MAX(dps.customer_apd) > 0 THEN 1 ELSE 0 END AS customer_apm,
   SUM(dps.product_cost_amount),
   SUM(dps.product_mrp_amount),
   SUM(dps.cancelled_product_qty),
   SUM(dps.cancelled_cost_amount),
   SUM(dps.cancelled_mrp_amount),
   SUM(dps.cancelled_order_apd),
   CASE WHEN MAX(dps.cancelled_order_apd) > 0 THEN 1 ELSE 0 END AS cancelled_order_apm,
   %s,  -- Replaced with parameter placeholder
   %s   -- Replaced with parameter placeholder
FROM devdw.daily_product_summary dps
LEFT JOIN devdw.monthly_product_summary mps
  ON TO_CHAR(dps.summary_date, 'YYYY-MM-01')::DATE = mps.start_of_the_month_date
  AND dps.dw_product_id = mps.dw_product_id
WHERE mps.dw_product_id IS NULL
GROUP BY TO_CHAR(dps.summary_date, 'YYYY-MM-01')::DATE, dps.dw_product_id;
"""
    ],

    'MONTHLY_CUSTOMER_SUMMARY' : [
        """
WITH CTE AS (
  SELECT TO_CHAR(dcs.summary_date, 'YYYY-MM-01')::DATE AS start_of_the_month_date,
         dcs.dw_customer_id,
         SUM(dcs.order_count) AS order_count,
         SUM(dcs.order_apd) AS order_apd,
         CASE
           WHEN SUM(dcs.order_apd) > 0 THEN 1
           ELSE 0
         END AS order_apm,
         SUM(dcs.order_amount) AS order_amount,
         SUM(dcs.order_cost_amount) AS order_cost_amount,
         SUM(dcs.order_mrp_amount) AS order_mrp_amount,
         SUM(dcs.products_ordered_qty) AS products_ordered_qty,
         SUM(dcs.products_items_qty) AS products_items_qty,
         SUM(dcs.cancelled_order_count) AS cancelled_order_count,
         SUM(dcs.cancelled_order_amount) AS cancelled_order_amount,
         SUM(dcs.cancelled_order_apd) AS cancelled_order_apd,
         CASE
           WHEN SUM(dcs.cancelled_order_apd) > 0 THEN 1
           ELSE 0
         END AS cancelled_order_apm,
         SUM(dcs.shipped_order_count) AS shipped_order_count,
         SUM(dcs.shipped_order_amount) AS shipped_order_amount,
         SUM(dcs.shipped_order_apd) AS shipped_order_apd,
         CASE
           WHEN SUM(dcs.shipped_order_apd) > 0 THEN 1
           ELSE 0
         END AS shipped_order_apm,
         SUM(dcs.payment_apd) AS payment_apd,
         CASE
           WHEN SUM(dcs.payment_apd) > 0 THEN 1
           ELSE 0
         END AS payment_apm,
         SUM(dcs.payment_amount) AS payment_amount,
         SUM(dcs.new_customer_apd) AS new_customer_apd,
         CASE
           WHEN SUM(dcs.new_customer_apd) > 0 THEN 1
           ELSE 0
         END AS new_customer_apm
  FROM devdw.daily_customer_summary dcs
  WHERE dcs.summary_date >= %s  -- etl_batch_date parameter
  GROUP BY TO_CHAR(dcs.summary_date, 'YYYY-MM-01')::DATE, dcs.dw_customer_id
) 
UPDATE devdw.monthly_customer_summary
SET 
    order_count = mcs.order_count + c.order_count,
    order_apd = mcs.order_apd + c.order_apd,
    order_apm = (mcs.order_apm::int | c.order_apm::int),  -- bitwise OR
    order_amount = mcs.order_amount + c.order_amount,
    order_cost_amount = mcs.order_cost_amount + c.order_cost_amount,
    order_mrp_amount = mcs.order_mrp_amount + c.order_mrp_amount,
    products_ordered_qty = mcs.products_ordered_qty + c.products_ordered_qty,
    products_items_qty = mcs.products_items_qty + c.products_items_qty,
    cancelled_order_count = mcs.cancelled_order_count + c.cancelled_order_count,
    cancelled_order_amount = mcs.cancelled_order_amount + c.cancelled_order_amount,
    cancelled_order_apd = mcs.cancelled_order_apd + c.cancelled_order_apd,
    cancelled_order_apm = (mcs.cancelled_order_apm::int | c.cancelled_order_apm::int),  -- bitwise OR
    shipped_order_count = mcs.shipped_order_count + c.shipped_order_count,
    shipped_order_amount = mcs.shipped_order_amount + c.shipped_order_amount,
    shipped_order_apd = mcs.shipped_order_apd + c.shipped_order_apd,
    shipped_order_apm = (mcs.shipped_order_apm::int | c.shipped_order_apm::int),  -- bitwise OR
    payment_apd = mcs.payment_apd + c.payment_apd,
    payment_apm = (mcs.payment_apm::int | c.payment_apm::int),  -- bitwise OR
    payment_amount = mcs.payment_amount + c.payment_amount,
    new_customer_apd = mcs.new_customer_apd + c.new_customer_apd,
    new_customer_apm = (mcs.new_customer_apm::int | c.new_customer_apm::int),  -- bitwise OR
    dw_update_timestamp = CURRENT_TIMESTAMP,
    etl_batch_no = %s,  -- etl_batch_no parameter
    etl_batch_date = %s  -- etl_batch_date parameter
FROM CTE c
JOIN devdw.monthly_customer_summary mcs
ON mcs.start_of_the_month_date = c.start_of_the_month_date
  AND mcs.dw_customer_id = c.dw_customer_id;
        """,
        """
        -- Insert new records into the monthly_customer_summary table
        INSERT INTO devdw.monthly_customer_summary (
            start_of_the_month_date,
            dw_customer_id,
            order_count,
            order_apd,
            order_apm,
            order_amount,
            order_cost_amount,
            order_mrp_amount,
            products_ordered_qty,
            products_items_qty,
            cancelled_order_count,
            cancelled_order_amount,
            cancelled_order_apd,
            cancelled_order_apm,
            shipped_order_count,
            shipped_order_amount,
            shipped_order_apd,
            shipped_order_apm,
            payment_apd,
            payment_apm,
            payment_amount,
            new_customer_apd,
            new_customer_apm,
            new_customer_paid_apd,
            new_customer_paid_apm,
            etl_batch_no,
            etl_batch_date
        )
        SELECT
            TO_CHAR(dcs.summary_date, 'YYYY-MM-01')::DATE AS start_of_the_month_date,
            dcs.dw_customer_id, 
            SUM(dcs.order_count),
            SUM(dcs.order_apd),
            CASE WHEN SUM(dcs.order_apd) > 0 THEN 1 ELSE 0 END AS order_apm,
            SUM(dcs.order_amount),
            SUM(dcs.order_cost_amount),
            SUM(dcs.order_mrp_amount),
            SUM(dcs.products_ordered_qty),
            SUM(dcs.products_items_qty),
            SUM(dcs.cancelled_order_count),
            SUM(dcs.cancelled_order_amount),
            SUM(dcs.cancelled_order_apd),
            CASE WHEN SUM(dcs.cancelled_order_apd) > 0 THEN 1 ELSE 0 END AS cancelled_order_apm,
            SUM(dcs.shipped_order_count),
            SUM(dcs.shipped_order_amount),
            SUM(dcs.shipped_order_apd),
            CASE WHEN SUM(dcs.shipped_order_apd) > 0 THEN 1 ELSE 0 END AS shipped_order_apm,
            SUM(dcs.payment_apd),
            CASE WHEN SUM(dcs.payment_apd) > 0 THEN 1 ELSE 0 END AS payment_apm,
            SUM(dcs.payment_amount),
            SUM(dcs.new_customer_apd),
            CASE WHEN SUM(dcs.new_customer_apd) > 0 THEN 1 ELSE 0 END AS new_customer_apm,
            0 AS new_customer_paid_apd,
            0 AS new_customer_paid_apm,
            %s,  -- etl_batch_no parameter
            %s   -- etl_batch_date parameter
        FROM devdw.daily_customer_summary dcs
        LEFT JOIN devdw.monthly_customer_summary mcs
            ON TO_CHAR(dcs.summary_date, 'YYYY-MM-01')::DATE = mcs.start_of_the_month_date
            AND dcs.dw_customer_id = mcs.dw_customer_id
        WHERE mcs.dw_customer_id IS NULL
        GROUP BY TO_CHAR(dcs.summary_date, 'YYYY-MM-01')::DATE, dcs.dw_customer_id;
        """
    ]
}