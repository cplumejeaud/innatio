select * 
from imhana2021."INAT_NAT_EPCI" ine ;

select unit, ine."Allemands"
from imhana2021."INAT_NAT_EPCI" ine 
where ine."Categorie" = 'Etranger' and ine."Indicateur" like 'INAT_NAT%';

select max(ine."Allemands"), avg("Allemands") , stddev("Allemands")
from imhana2021."INAT_NAT_EPCI" ine 
where ine."Categorie" = 'Etranger' and ine."Indicateur" like 'INAT_NAT%';
-- 11284.0	69.69532554257096	420.08617466763474

select * from INAT_NAT_EPCI;

select * from nat_epci;

alter table imhana2021.indicateurs add column code02 text;
alter table imhana2021.indicateurs add column modalite02 text;
alter table imhana2021.indicateurs add column libelle02 text;

alter table imhana2021.indicateurs add column code03 text;
alter table imhana2021.indicateurs add column modalite03 text;
alter table imhana2021.indicateurs add column libelle03 text;

select indicateur, code01 from indicateurs;

alter table imhana2021.indicateurs add  primary key(indicateur, modalites);

set search_path='imhana2021', public;

select count(*) from nat_epci_wide t ;

select * from nat_epci_wide t where t.indicateur = 'LNAIE' limit 100;
-- delete from nat_epci_wide t where t.indicateur = 'LNAIE' limit 100;
select unit, t."indicateurMode" , t."Britanniques", t.immigres, t.etrangers 
from nat_epci_wide t 
where t.indicateur = 'ARRIVR' limit 100;

select t."indicateurMode" , sum(t."Britanniques") as anglais, sum(t.immigres) as total_immi, sum(t.etrangers) as total_etrangers, sum(t."francaisParAcquisition" ) as total_naturalises
from nat_epci_wide t 
where t.indicateur = 'ARRIVR' 
group by t."indicateurMode";

select t."indicateurMode" , sum(t."Britanniques") as anglais, sum(t.immigres) as total_immi, sum(t.etrangers) as total_etrangers, sum(t."francaisParAcquisition" ) as total_naturalises
from nat_epci_wide t 
where t.indicateur = 'ARRIVR' and unit in ('200071819', '242401024', '242400752')
group by t."indicateurMode";


select count(*) from nat_epci_wide t where t."indicateurCode"  = 'SEXE';

alter table nat_epci rename column "NAT2"  to NAT2 ;
alter table nat_epci rename column "anneeRp"  to anneeRp ;
alter table nat_epci rename column "indicateur"  to indicateur ;
alter table nat_epci rename column "indicateurCode"  to indicateurCode ;
alter table nat_epci rename column "indicateurMode"  to indicateurMode ;
-- alter table nat_epci rename column "Ensemble"  to Ensemble ;

alter table nat_epci add column "Etranger" int ;
alter table nat_epci add column "Français par acquisition" int;
alter table nat_epci add column "Français de naissance" int;

'Etranger', 'Français par acquisition', 'Français de naissance'


select * from nat_epci where NAT2 = 'Algériens' and "indicateur" = 'NAT' and unit= '200000172' and "anneeRp" = 2021 and "Ensemble" is not null;

select * from inat_nat where "NAT2" = 'Algériens' and "indicateur" = 'NAT' and unit= '200000172' and "anneeRp" = 2021 and "Etranger" is not null;

select * from nat_epci where NAT2 = 'Algériens' and indicateur = 'NAT' and unit= '200000172' and "anneeRp" = 2021 and "Ensemble" is not null;
select * from nat_epci where NAT2 = 'Algériens' and indicateurCode = 'NAT2' and unit= '200000172' and anneeRp = 2021 and "Ensemble" is not null;


--, connect_args={'options': '-csearch_path={}'.format('imhana2021,public')}
--update nat_epci n set "Etranger" = inalter."Etranger", 
--	"Français par acquisition" = inalter."Français par acquisition",
--	"Français de naissance" = inalter."Français de naissance"
--from inat_nat inalter 
--where n.unit = inalter.unit 
--and n."NAT2" = inalter."NAT2"
--and n."anneeRp = inalter."anneeRp"
--and n."indicateurcode" = inalter."indicateurcode"
--and n."indicateurmode" = inalter."indicateurmode"


select * from fusion_epci where "NAT2" = 'Tous';

select * from basicfusion_epci where "Immigrés" != "PremiereGeneration" and "NAT2" != 'Français';

alter table basicfusion_epci add CONSTRAINT basicfusion_epci_pk PRIMARY KEY (unit,indicateur,"indicateurMode","NAT2","anneeRp");
-- 1 min 7 s
alter table basicfusion_epci drop CONSTRAINT basicfusion_epci_pk ;
--PRIMARY KEY (unit,indicateur,"indicateurMode","NAT2","anneeRp");

-- Très important : fait après insertion le 15 avril soir
select indicateur||'_'||"indicateurMode" from basicfusion_epci;
update basicfusion_epci set indicateur = indicateur||'_'||"indicateurMode";
-- 1 min 34 pour 20 231 640 lignes

alter table basicfusion_epci add CONSTRAINT basicfusion_epci_pk PRIMARY KEY (unit,indicateur,"NAT2","anneeRp");

alter table geoepci_demo add CONSTRAINT geoepci_demo_epci_pk PRIMARY KEY ("CODE_SIREN","INSEE_REG");

alter table indicateurs add CONSTRAINT indicateurs_pk PRIMARY KEY ("indicateur","modalites");

alter table indicateurs add CONSTRAINT indicateurs_pk PRIMARY KEY ("indicateur","modalites");

--------------------------------------------------------
-- le 16 avril 2026
--------------------------------------------------------

set search_path = imhana_epci, imhana_communes, imhana2021, public;

create schema imhana_epci;
create schema imhana_communes;

alter table imhana2021.basicfusion_epci set schema imhana_epci;
alter table imhana2021.indicateurs  set schema imhana_epci;
alter table imhana2021.geoepci_demo   set schema imhana_epci;
alter table imhana2021.nat_epci_wide    set schema imhana_epci;
alter table imhana2021.nat_epci    set schema imhana_epci;

alter table imhana_epci.basicfusion_epci rename to nat_epci_long;

select distinct "indicateur" from imhana_epci.nat_epci_long where "NAT2" = 'Tous';
-- aucune ligne

select * from imhana_epci.nat_epci_long where "NAT2" = 'Tous';
--O

select distinct "NAT2" from imhana_epci.nat_epci_long order by "NAT2";

select distinct "indicateurCode" from imhana_epci.nat_epci_long order by "indicateurCode";

select distinct "indicateur" from imhana_epci.resumes_nat_epci_long order by "indicateur";

select * from resumes_nat_epci_long r where r.etrangers  < 50 and indicateur = 'NAT';
select * from resumes_nat_epci_long r where r.immigres  < 71 and indicateur = 'NAT';
-- 247600604	CC de Londinières  5192

select min(r.etrangers), min(r.immigres ) from resumes_nat_epci_long r where  indicateur = 'NAT'; 
-- 40, 245400759	CC du Pays du Sanon
select 40 / 5833.0 * 100; -- 0.68 %
select 70 /  5192.0 * 100;-- 1.34 %

select distinct "indicateurCode" from imhana_epci.resumes_nat_epci_long order by "indicateurCode";

alter table nat_epci_long add CONSTRAINT nat_epci_long_pk PRIMARY KEY (unit,indicateur,"NAT2","anneeRp");
-- 6 min 25

select * from imhana_epci.nat_epci_long 
where "indicateurCode" = 'VOIT'
and unit='242400752' and "NAT2" in ('Français', 'Britanniques', 'Néerlandais', 'Belges');

select * from imhana_epci.resumes_nat_epci_long 
where "indicateurCode" = 'VOIT'
and unit='242400752'
order by "indicateurCode";


alter table nat_epci add CONSTRAINT nat_epci_pk PRIMARY KEY (unit,"NAT2","anneeRp");
alter table nat_epci_wide add CONSTRAINT nat_epci_wide_pk PRIMARY KEY (unit, indicateur, categorie, "anneeRp");
-- 4.9s
alter table nat_epci_wide drop column "NOM";

update nat_epci_wide set indicateur = indicateur||'.'||"indicateurMode";
-- 32 s
select indicateur||'.'||"indicateurMode" from nat_epci_wide;

select indicateur, "indicateurCode"||'.'||"indicateurMode" from nat_epci_long where "indicateurCode" = 'SEXE';
-- where "indicateurCode" = 'NAT2';
update nat_epci_long set indicateur = "indicateurCode"||'.'||"indicateurMode" ;
-- 33 343 674 lignes et 33 min 12s
