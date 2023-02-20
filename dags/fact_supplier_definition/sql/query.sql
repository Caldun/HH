with 
    ostatki as (
    select 
        nom."Наименование" as Товар
        ,t1."Товар" as nomenclatureGuid
        ,nom."Код"
        ,t1."КлючУникальности"
    	,sklad."Наименование" as Склад
        ,sum(t1."СуммаБезНДС") as СуммаБезНДС
        ,sum(t1."Количество") as Количество
        ,max(osnt."Дата") as ДатаПриема
    from "cdc.adm-1c-dns-m.dns_m"."RN.Schet_41_3_4.Itogi" as t1
    left join "cdc.adm-1c-dns-m.dns_m"."S.Nomenklatura" as nom
    on t1."Товар" = nom."Ссылка"
    left join "cdc.adm-1c-dns-m.dns_m"."S.SkladyFiliala" as sklad
    on t1."Склад" = sklad."Ссылка" 
    left join "cdc.adm-1c-dns-m.dns_m"."D.OperatsiiSneispravnymTovarom.Sostav" as osnt_s
    on t1."КлючУникальности" = osnt_s."КлючУникальности" 
    left join "cdc.adm-1c-dns-m.dns_m"."D.OperatsiiSneispravnymTovarom" as osnt
    on osnt_s."Ссылка" = osnt."Ссылка" and osnt."ОперацияДокумента" = 'a7a93b36-a077-11e1-b29d-001cc06fb405'
    where t1."Период" = '3999-11-01 00:00:00.000'  and t1."Филиал" = 'aeb7d39b-535b-49e6-bad7-5074331b29c1' and t1."Статус" = '02c3f4a8-b493-4e5c-86b5-4ba2f7059ade'
    group by nom."Наименование", t1."Товар", nom."Код", t1."КлючУникальности", sklad."Наименование" 
    ),
    RabotaSBrakom_Max as (
    select 
        t1."КлючУникальности" 
        ,t2."ОперацияДокумента"
        ,max(t2."Дата") as "Дата"
    from "cdc.adm-1c-dns-m.dns_m"."D.RabotaSBrakom.Sostav" as t1
    inner join "cdc.adm-1c-dns-m.dns_m"."D.RabotaSBrakom" as t2
    on t1."Ссылка" = t2."Ссылка"
    inner join ostatki as t3
    on t1."КлючУникальности" = t3."КлючУникальности" and t2."Дата" > t3.ДатаПриема
    where t2."ОперацияДокумента" in ('5652bd16-69af-11e6-a720-00155d033307', '5652bd17-69af-11e6-a720-00155d033307')
    and t2."Проведен" = 1
    group by t1."КлючУникальности", t2."ОперацияДокумента"
    ),
    RabotaSBrakom as ( 
    select 
        t2."Ссылка" 
        ,concat('Работа с браком ', t2."Номер", ' от ', to_char(t2."Дата", 'dd.mm.yyyy HH24:MI:SS')) as Doc
        ,t2."ОперацияДокумента" 
        ,t2."Дата" 
        ,t3."Партия"
        ,t3."КлючУникальности"
        ,t3."SN"
    from RabotaSBrakom_Max as t1
    inner join "cdc.adm-1c-dns-m.dns_m"."D.RabotaSBrakom" as t2
    on t1."ОперацияДокумента" = t2."ОперацияДокумента" and t1."Дата" = t2."Дата" 
    inner join "cdc.adm-1c-dns-m.dns_m"."D.RabotaSBrakom.Sostav" as t3
    on t2."Ссылка" = t3."Ссылка" and t1."КлючУникальности" = t3."КлючУникальности" 
    )
    select
        timezone('Europe/Moscow', now())::timestamp as load_date 
        ,t1.Товар as product
        ,t1.nomenclatureGuid as nomenclatureGuid
        ,case when t_Op."SN" is null then lower(t_Sogl."SN") else lower(t_Op."SN") end as serialNumber
        ,t1."Код" as code
        ,t_Op.Doc as doc_op
        ,t_Op."Дата" as date_doc_op
        ,t_Sogl.Doc as doc_sog
        ,t_Sogl."Дата" as date_doc_sog 
        ,case when t_Sogl."Дата" is not null then timezone('Europe/Moscow', now())::date - t_Sogl."Дата"::date 
        	  when t_Op."Дата" is not null then timezone('Europe/Moscow', now())::date - t_Op."Дата"::date 
        	  else timezone('Europe/Moscow', now())::date - t1.ДатаПриема::date 
        	  end time_since_last_status
        ,t1.СуммаБезНДС as sum_with_out_vat
        ,t1.Склад as storehouse
        ,t1.ДатаПриема as date_osnt
    from ostatki as t1
    left join RabotaSBrakom as t_Op
    on t1."КлючУникальности" = t_Op."КлючУникальности" and t_Op."ОперацияДокумента" = '5652bd16-69af-11e6-a720-00155d033307'
    left join RabotaSBrakom as t_Sogl
    on t1."КлючУникальности" = t_Sogl."КлючУникальности" and t_Sogl."ОперацияДокумента" = '5652bd17-69af-11e6-a720-00155d033307'
    where t1.Количество > 0 
