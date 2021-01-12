-- Q.01
SELECT T.name
FROM Trainer AS T, CatchedPokemon C
WHERE T.id = C.owner_id
GROUP BY T.name
HAVING COUNT(C.id) >= 3
ORDER BY COUNT(C.id) DESC;

-- Q.02
SELECT P.name, P.type
FROM Pokemon P
WHERE P.type IN (
   SELECT P2.type FROM Pokemon P2 GROUP BY P2.type
   HAVING COUNT(*) >= (SELECT MAX(tmp.num)
   FROM (SELECT COUNT(*) AS num
   FROM Pokemon P3 GROUP BY P3.type) as tmp
   WHERE tmp.num NOT IN (SELECT MAX(tmp2.num2) 
   FROM (SELECT COUNT(*) AS num2 FROM Pokemon P4
   GROUP BY P4.type) as tmp2))
  )
ORDER BY P.name;

-- Q.03
SELECT AVG(C.level)
FROM Trainer AS T, CatchedPokemon AS C, Pokemon As P
WHERE T.id = C.owner_id AND C.pid = P.id
AND T.hometown = 'Sangnok City' AND P.type = 'Electric';

-- Q.04
SELECT C.nickname
FROM CatchedPokemon AS C
WHERE C.level >= 50
ORDER BY C.nickname;

-- Q.05
SELECT T1.name
FROM Trainer AS T1
WHERE T1.id NOT IN (
  SELECT T2.id
  FROM Trainer AS T2, Gym AS G
  WHERE T2.id = G.leader_id)
ORDER BY T1.name;

-- Q.06
SELECT T.name
FROM Trainer AS T
WHERE T.hometown = 'Blue City'
ORDER BY T.name;

-- Q.07
SELECT T.name
FROM Trainer AS T
ORDER BY T.hometown;

-- Q.08
SELECT AVG(C.level)
FROM Trainer AS T, CatchedPokemon AS C
WHERE T.id = C.owner_id AND T.name = 'Red';

-- Q.09
SELECT T.name, AVG(C.level)
FROM Trainer AS T, Gym AS G, CatchedPokemon AS C
WHERE T.id = G.leader_id AND T.id = C.owner_id
GROUP BY T.name
ORDER BY T.name;

-- Q.10
SELECT C.nickname
FROM CatchedPokemon C
WHERE C.level >= 50 AND C.owner_id >= 6
ORDER BY C.nickname;

-- Q.11
SELECT P.id, P.name
FROM Pokemon AS P
ORDER BY P.id;

-- Q.12
SELECT DISTINCT P.name, P.type
FROM Pokemon AS P, CatchedPokemon AS C
WHERE P.id = C.pid AND C.level >= 30
ORDER BY P.name;

-- Q.13
SELECT P.name, P.id
FROM Trainer AS T, Pokemon AS P, CatchedPokemon AS C
WHERE T.id = C.owner_id AND P.id = C.pid 
AND T.hometown = 'Sangnok City'
ORDER BY P.id;

-- Q.14
SELECT P.name
FROM Pokemon P, Evolution E
WHERE P.id = E.before_id AND P.type = 'Grass';

-- Q.15
SELECT T.id, COUNT(*)
FROM Trainer T, CatchedPokemon C
WHERE T.id = C.owner_id
GROUP BY T.name
HAVING COUNT(*) = (SELECT MAX(result.tmp) FROM (SELECT COUNT(*) AS tmp FROM Trainer T2, CatchedPokemon C2 WHERE T2.id = C2.owner_id GROUP BY T2.name) AS result)
ORDER BY T.id;

-- Q.16
SELECT COUNT(*)
FROM Pokemon P
WHERE P.type IN ('Water', 'Electric', 'Psychic');

-- Q.17
SELECT COUNT(DISTINCT P.type)
FROM Trainer AS T, Pokemon AS P, CatchedPokemon AS C
WHERE T.id = C.owner_id AND P.id = C.pid
AND T.hometown = 'Sangnok City';

-- Q.18
SELECT AVG(C.level)
FROM Trainer AS T, Gym AS G, CatchedPokemon AS C
WHERE T.id = C.owner_id AND T.id = G.leader_id;

-- Q.19
SELECT COUNT(DISTINCT P.type)
FROM Gym AS G, CatchedPokemon AS C, Pokemon AS P
WHERE G.leader_id = C.owner_id AND P.id = C.pid
AND G.city = 'SangNok City';

-- Q.20
SELECT T.name, COUNT(C.id)
FROM Trainer AS T, CatchedPokemon AS C
WHERE T.id = C.owner_id AND T.hometown = 'Sangnok City'
GROUP BY T.name
ORDER BY COUNT(C.id);

-- Q.21
SELECT T.name, COUNT(*)
FROM Trainer AS T, Gym AS G, CatchedPokemon AS C
WHERE T.id = G.leader_id AND T.id = C.owner_id
GROUP BY T.name
ORDER BY T.name;

-- Q.22
SELECT P.type, COUNT(*)
FROM Pokemon P
GROUP BY P.type
ORDER BY COUNT(*), P.type;

-- Q.23
SELECT DISTINCT T.name
FROM Trainer AS T, CatchedPokemon AS C
WHERE T.id = C.owner_id AND C.level <= 10
ORDER BY T.name;

-- Q.24
SELECT T.hometown, AVG(C.level)
FROM Trainer AS T, CatchedPokemon AS C
WHERE T.id = C.owner_id
GROUP BY T.hometown
ORDER BY AVG(C.level);

-- Q.25
SELECT P.name
FROM Pokemon P
WHERE P.id IN(
  SELECT DISTINCT C2.pid
  FROM Trainer T2, CatchedPokemon C2
  WHERE T2.id = C2.owner_id
  AND T2.hometown = 'Sangnok City')
AND P.id IN(
  SELECT DISTINCT C3.pid
  FROM Trainer T3, CatchedPokemon C3
  WHERE T3.id = C3.owner_id
  AND T3.hometown = 'Brown City')
ORDER BY P.name;

-- Q.26
SELECT P.name
FROM Pokemon P, CatchedPokemon C
WHERE P.id = C.pid AND C.nickname LIKE '% %'
ORDER BY P.name DESC;

-- Q.27
SELECT T.name, MAX(C.level)
FROM Trainer T, CatchedPokemon C
WHERE T.id = C.owner_id
GROUP BY T.name
HAVING COUNT(C.id) >= 4
ORDER BY T.name;

-- Q.28
SELECT T.name, AVG(C.level) AS avgLevel
FROM Trainer T, CatchedPokemon C
WHERE T.id = C.owner_id AND C.pid IN (
  SELECT id
  FROM Pokemon
  WHERE type = 'Normal' OR type = 'Electric')
GROUP BY T.name
ORDER BY avgLevel;

-- Q.29
SELECT P.type, COUNT(*)
FROM Pokemon P, CatchedPokemon C
WHERE P.id = C.pid
GROUP BY P.type
ORDER BY P.type;

-- Q.30
SELECT DISTINCT (SELECT id FROM Pokemon WHERE id IN (SELECT before_id FROM Evolution WHERE after_id = P.id)) AS FIrst_Id, (SELECT name FROM Pokemon WHERE id IN (SELECT before_id FROM Evolution WHERE after_id = P.id)) AS First
, (SELECT name FROM Pokemon WHERE id = P.id) AS Second, (SELECT name FROM Pokemon WHERE id IN (SELECT after_id FROM Evolution WHERE before_id = P.id)) AS Third
FROM Pokemon P, Evolution E
WHERE P.id IN (
  SELECT id
  FROM Pokemon, Evolution
  WHERE id = before_id)
AND P.id IN (
  SELECT id
  FROM Pokemon, Evolution
  WHERE id = after_id)
ORDER BY First_Id;

-- Q.31
SELECT P.type
FROM Pokemon P, Evolution E
WHERE P.id = E.before_id OR P.id = E.after_id
GROUP BY P.type
HAVING COUNT(DISTINCT P.id) >= 3
ORDER BY P.type DESC;


-- Q.32
SELECT name
FROM Pokemon
WHERE id NOT IN (
  SELECT DISTINCT C.pid
  FROM CatchedPokemon C)
ORDER BY name;

-- Q.33
SELECT SUM(C.level)
FROM Trainer T, CatchedPokemon C
WHERE T.id = C.owner_id AND T.name = 'Matis';

-- Q.34
SELECT P.name, C.level, C.nickname
FROM Gym G, Pokemon P, CatchedPokemon C
WHERE G.leader_id = C.owner_id AND P.id = C.pid
AND C.nickname LIKE 'A%'
ORDER BY P.name DESC;

-- Q.35
SELECT P.name
FROM Pokemon P, Evolution E
WHERE P.id = E.before_id AND E.before_id > E.after_id
ORDER BY P.name;

-- Q.36
SELECT DISTINCT T.name
FROM Trainer T, CatchedPokemon C, Evolution E
WHERE T.id = C.owner_id AND C.pid = E.after_id 
AND E.after_id NOT IN (SELECT before_id FROM Evolution)
ORDER BY T.name;

-- Q.37
SELECT T.name, SUM(C.level)
FROM Trainer T, CatchedPokemon C
WHERE T.id = C.owner_id
GROUP BY T.name
HAVING SUM(C.level) = (
  SELECT MAX(levelSum)
  FROM (SELECT SUM(C2.level) AS levelSum
    FROM Trainer T2, CatchedPokemon C2
    WHERE T2.id = C2.owner_id
    GROUP BY T2.name) AS result
  )
ORDER BY T.name;

-- Q.38
SELECT P.name
FROM Pokemon P, Evolution E
WHERE P.id = E.after_id AND P.id NOT IN (
  SELECT before_id
  FROM Evolution)
ORDER BY P.name;

-- Q.39
SELECT DISTINCT T.name
FROM Trainer T, CatchedPokemon C
WHERE T.id = C.owner_id
GROUP BY T.name, C.pid
HAVING COUNT(*) >= 2
ORDER BY T.name;

-- Q.40
SELECT Ci.name, C.nickname
FROM City Ci, Trainer T, CatchedPokemon C, 
(SELECT Ci2.name AS maxName, MAX(C2.level) AS maxLevel
 FROM City Ci2, Trainer T2, CatchedPokemon C2
 WHERE Ci2.name = T2.hometown AND T2.id = C2.owner_id
 GROUP BY Ci2.name) AS max
WHERE Ci.name = T.hometown AND T.id = C.owner_id
AND Ci.name = maxName AND C.level = maxLevel
ORDER BY Ci.name;

