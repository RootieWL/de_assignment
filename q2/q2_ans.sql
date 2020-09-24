--- q1a)
select b.book_id, m.publisher
from books b
left join (select * from members
           where member_type = 'author') m 
on b.member_id = m.member_id;

--- q1b)
select m.country_code, 
       count(distinct m.member_id) as reviewers
from members m
where m.member_type = 'reviewer';

--- q1c)
select m.owner_citizenship_code, 
       count(distinct m.member_id) as reviewers
from members m
where m.member_type = 'reviewer';

--- q1d)
with temp as (
select r.member_id, 
       count(r.book_id) as books_reviewed
from reviewers r
where year(r.review_date) = 2018
group by r.member_id
having count(r.book_id) >= 10)

select count(member_id)
from temp;

--- q1e)
select r.member_id, 
       count(r.book_id) as review_count,
       case when count(r.book_id) > 100 then 'A category'
            when count(r.book_id) > 50 and count(r.book_id) <= 100 
                 then 'B category'
            else 'C category' end as review_category
from reviewers r
group by r.member_id;


--- q1f)
with temp as (
select rank() over(partition by month(r.review_date), r.member_id
                   order by count(r.book_id) desc) as row_number,
       r.member_id, 
       month(r.review_date) as month_reviewed,
       count(r.book_id) as review_count
from reviewers r
group by r.member_id, month(r.review_date)
order by month(r.review_date) asc, count(r.book_id) desc)

select month_reviewed, member_id, review_count
from temp 
where row_number <= 2;
