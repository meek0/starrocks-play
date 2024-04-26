select o.hgvsg, o.variant_class, o.symbol, o.consequence, o.vep_impact, c.interpretations, g.phenotype_inheritance_code, o.gnomad_af, o.quality, o.zygosity from occurrences o left join clinvar c on o.chromosome=c.chromosome and o.start=c.start and o.reference=c.reference and o.alternate=c.alternate left join genes g on o.symbol = g.symbol where o.sample_id = %s limit 20;