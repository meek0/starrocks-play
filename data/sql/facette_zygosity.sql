select zygosity, count(1) as c from normalized_snv where sample_id = %s group by zygosity;