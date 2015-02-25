DROP VIEW IF EXISTS q1a, q1b, q1c, q1d, q2, q3, q4, q5, q6, q7;

-- Question 1a
CREATE VIEW q1a(id, amount)
AS
  SELECT H.cmte_id, H.transaction_amt 
  From committee_contributions H
  Where H.transaction_amt > 5000 
;

-- Question 1b
CREATE VIEW q1b(id, name, amount)
AS
  SELECT H.cmte_id, H.name, H.transaction_amt
  From committee_contributions H
  Where H.transaction_amt > 5000 
;

-- Question 1c
CREATE VIEW q1c(id, name, avg_amount)
AS
  SELECT H.id, H.name, avg(H.amount)
  From q1b H
  Group by H.id, H.name
;

-- Question 1d
CREATE VIEW q1d(id, name, avg_amount)
AS
  SELECT H.id, H.name, H.avg_amount
  From q1c H
  Where H.avg_amount > 10000
;

-- Question 2
CREATE VIEW q2(from_name, to_name)
AS
  WITH DemocraticParty(id, com_name) as 
  (Select H.id, H.name 
  From committees H
  Where H.pty_affiliation='DEM'),
  draft_Result(from_name, to_name, amount) as 
  (Select C.com_name, D.com_name, I.transaction_amt
  From intercommittee_transactions I, DemocraticParty D, DemocraticParty C
  Where I.cmte_id = D.id And I.other_id = C.id)
  Select N.from_name, N.to_name 
  from draft_Result N
  group by N.from_name, N.to_name
  order by SUM(N.amount) Desc
  limit 10
;

-- Question 3
CREATE VIEW q3(name)
AS
  WITH ObamaID(id) as
  (Select A.id 
  From candidates A 
  where lower(A.name) like '%obama%')
  Select Distinct C.name
  from committee_contributions B, committees C
  where B.cand_id Not in (select id from ObamaID) and B.cmte_id = C.id
  
;

-- Question 4.
CREATE VIEW q4 (name)
AS
  with draft_data as 
  (select A.cand_id from committee_contributions A group by A.cmte_id, A.cand_id),
  unique_id_count(num) as 
  (select count(A.id) from committees A group by A.id), 
  TotalCommittee(total) as 
  (select sum(B.num) from unique_id_count B), 
  candidate_count(id, number) as 
  (select A.cand_id, count(A.cand_id) from draft_data A group by A.cand_id)
  select D.name from candidate_count C, TotalCommittee T, candidates D where (C.number/T.total)*100 > 1 and C.id = D.id;
;

-- Question 5
CREATE VIEW q5 (name, total_pac_donations) AS
  SELECT 1,1 -- replace this line
;

-- Question 6
CREATE VIEW q6 (id) AS
  SELECT 1 -- replace this line
;

-- Question 7
CREATE VIEW q7 (cand_name1, cand_name2) AS
  SELECT 1,1 -- replace this line
;
