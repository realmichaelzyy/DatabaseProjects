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
  Where lower(H.pty_affiliation) like 'dem' group by H.id, H.name),
  draft_Result(from_name, to_name, amount) as 
  (Select C.com_name, D.com_name, I.transaction_amt
  From intercommittee_transactions I, DemocraticParty D, DemocraticParty C
  Where C.id=I.other_id and D.id=I.cmte_id)
  Select N.from_name, N.to_name 
  from draft_Result N
  group by N.from_name, N.to_name
  order by count(N.from_name) Desc
  limit 10
;

-- Question 3
CREATE VIEW q3(name)
AS
  WITH ObamaID(id) as
  (Select A.id 
  From candidates A 
  where lower(A.name) like '%obama%'),
  ObamaMoney(id) as 
  (Select Distinct B.cmte_id
  from committee_contributions B
  where B.cand_id in (select id from ObamaID))
  Select name
  from committees 
  where id not in (select id from ObamaMoney)
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
  WITH org_trans(id, amount) as 
  (select I.cmte_id, sum(I.transaction_amt) 
  from individual_contributions I
  where lower(I.entity_tp) like '%org%' 
  group by I.cmte_id)
  select S.name, I.amount 
  from committees S LEFT OUTER JOIN org_trans I
  on S.id = I.id
;

-- Question 6
CREATE VIEW q6 (id) AS
  with pac_id(id) as 
  (select cand_id from committee_contributions 
  where lower(entity_tp) like '%pac%' and cand_id is not null)
  select Distinct cand_id from committee_contributions
  where lower(entity_tp) like '%ccm%' and cand_id is not null and cand_id in (select * from pac_id)
;

-- Question 7
CREATE VIEW q7 (cand_name1, cand_name2) AS
  SELECT 1,1 -- replace this line
;
