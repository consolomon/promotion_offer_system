CREATE TABLE IF NOT EXISTS public.subscribers_restaurants (
	id int4 NOT NULL DEFAULT nextval('subscribers_restaraunts_id_seq'::regclass),
	client_id varchar NULL,
	restaurant_id varchar NULL
);

CREATE TABLE IF NOT EXISTS public.subscribers_feedback (
	id serial4 NOT NULL,
	restaraunt_id text NOT NULL,
	adv_campaign_id text NOT NULL,
	adv_campaign_content text NOT NULL,
	adv_campaign_owner text NOT NULL,
	adv_campaign_owner_contact text NOT NULL,
	adv_campaign_datetime_start int8 NOT NULL,
	adv_campaign_datetime_end int8 NOT NULL,
	datetime_created int8 NOT NULL,
	client_id text NOT NULL,
	trigger_datetime_created int4 NOT NULL,
	feedback varchar NULL,
	CONSTRAINT id_pk PRIMARY KEY (id)
);