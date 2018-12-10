--Well, imagine that you need to perform clustering on you customer 
--data on a regular basis as new customers sign up to keep an 
--updated understanding of customer behavior. 
--In this example, we might want to send out promotion emails and 
--can select the email addresses of customers in cluster 3 to 
--send out a promotion.

--You can also schedule jobs that run the stored procedure and 
--automatically send the results to for example a CRM application 
--or a reporting tool.

--The code below is selecting the email addresses of customers in 
--cluster 0, for a promotion campaign intending to activate 
--this group of customers:


USE [tpcxbb_1gb]
GO
--Get email addresses of customers in cluster 0 for a promotion campaign
SELECT customer.[c_email_address], customer.c_customer_sk
  FROM dbo.customer
  JOIN
  [dbo].[py_customer_clusters] as c
  ON c.Customer = customer.c_customer_sk
  WHERE c.cluster = 0

