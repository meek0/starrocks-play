select chromosome, count(1) as c from occurrences where sample_id = %s group by chromosome;