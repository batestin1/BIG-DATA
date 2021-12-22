#!/usr/local/bin/python3
#coding: utf-8
# PERSONA

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: nasa
#     Repositorio: stagin bronze
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
# imports
# imports
import json
from pyspark.sql import SparkSession
import pyspark.sql.functions as sfunc
import pyspark.sql.types as stypes
import pymongo
from pymongo import MongoClient
client = pymongo.MongoClient('localhost', 27017)


spark = SparkSession.builder.appName("MyApp").config("spark.mongodb.input.uri", "mongodb://localhost:27017").config("spark.mongodb.output.uri", "mongodb://localhost:27017").config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1").master("local").getOrCreate()

# extract
df = spark.read.format("mongo").option("database", "nasa").option("collection", "archive").load()

#transform
df.createOrReplaceTempView('df')

print("Starting processing for Bronze file...")


#nasa
nasa = spark.sql("""SELECT
CAST(identification.keipID AS STRING) as keipID,
CAST(identification.koi_name AS STRING) as koi_name,
CAST(exoplanet_information.kepler_name AS STRING) as kepler_name ,
CAST(exoplanet_information.exoplanet_disposition AS STRING) as exoplanet_disposition,
CAST(exoplanet_information.vetting_status AS STRING) as vetting_status,
CAST(exoplanet_information.date_of_last_parameter_update AS STRING) as date_of_last_parameter_update,
CAST(project_disposition.disposition_using_kepler_data AS STRING) as disposition_using_kepler_data,
CAST(project_disposition.disposition_score AS STRING) as disposition_score,
CAST(project_disposition.not_transit_like_false_positive_flag AS STRING) as not_transit_like_false_positive_flag,
CAST(project_disposition.stellar_eclipse_false_positive_flag AS STRING) as stellar_eclipse_false_positive_flag,
CAST(project_disposition.centroid_offset_false_positive_flag AS STRING) as centroid_offset_false_positive_flag,
CAST(project_disposition.ephemeris_math_indicates_contamination_false_positive_flag AS STRING) as ephemeris_math_indicates_contamination_false_positive_flag,
CAST(project_disposition.disposition_provenance AS STRING) as disposition_provenance,
CAST(transit_properties.orbital_period.orbital_period_days AS STRING) as orbital_period_days,
CAST(transit_properties.orbital_period.orbital_period_upper_unc_days AS STRING) as orbital_period_upper_unc_days,
CAST(transit_properties.orbital_period.orbital_period_lower_unc_days AS STRING) as orbital_period_lower_unc_days,
CAST(transit_properties.transit_epoch_bkjd.transit_epoch_bkjd AS STRING) as transit_epoch_bkjd,
CAST(transit_properties.transit_epoch_bkjd.transit_epoch_upper_unc_bkjd AS STRING) as transit_epoch_upper_unc_bkjd,
CAST(transit_properties.transit_epoch_bkjd.transit_epoch_lower_unc_bkjd AS STRING) as transit_epoch_lower_unc_bkjd,
CAST(transit_properties.transit_epoch_bjd.transit_epoch_bjd AS STRING) as transit_epoch_bjd,
CAST(transit_properties.transit_epoch_bjd.transit_epoch_upper_unc_bjd AS STRING) as transit_epoch_upper_unc_bjd,
CAST(transit_properties.transit_epoch_bjd.transit_epoch_lower_unc_bjd AS STRING) as transit_epoch_lower_unc_bjd,
CAST(transit_properties.eccentricity.eccentricity AS STRING) as eccentricity,
CAST(transit_properties.eccentricity.eccentricity_upper_unc AS STRING) as eccentricity_upper_unc,
CAST(transit_properties.eccentricity.eccentricity_lower_unc AS STRING) as eccentricity_lower_unc,
CAST(transit_properties.long_periastron.long_periastron_deg AS STRING) as long_periastron_deg,
CAST(transit_properties.long_periastron.long_periastron_upper_unc_deg AS STRING) as long_periastron_upper_unc_deg,
CAST(transit_properties.long_periastron.long_periastron_lower_unc_deg AS STRING) as long_periastron_lower_unc_deg,
CAST(transit_properties.impact_parameter.impact_parameter AS STRING) as impact_parameter,
CAST(transit_properties.impact_parameter.impact_parameter_upper_unc AS STRING) as impact_parameter_upper_unc,
CAST(transit_properties.impact_parameter.impact_parameter_lower_unc AS STRING) as impact_parameter_lower_unc,
CAST(transit_properties.transit_duration.transit_duration_hrs AS STRING) as transit_duration_hrs,
CAST(transit_properties.transit_duration.transit_duration_upper_unc_hrs AS STRING) as transit_duration_upper_unc_hrs,
CAST(transit_properties.transit_duration.transit_duration_lower_unc_hrs AS STRING) as transit_duration_lower_unc_hrs,
CAST(transit_properties.ingress_duration.ingress_duration_hrs AS STRING) as ingress_duration_hrs,
CAST(transit_properties.ingress_duration.ingress_duration_upper_unc AS STRING) as ingress_duration_upper_unc,
CAST(transit_properties.ingress_duration.ingress_duration_lower_unc AS STRING) as ingress_duration_lower_unc,
CAST(transit_properties.transit_depth.transit_depth_ppm AS STRING) as transit_depth_ppm,
CAST(transit_properties.transit_depth.transit_depth_upper_unc_ppm AS STRING) as transit_depth_upper_unc_ppm,
CAST(transit_properties.transit_depth.transit_depth_lower_unc_ppm AS STRING) as transit_depth_lower_unc_ppm,
CAST(transit_properties.planet_star_radius_ratio.planet_star_radios_ratio AS STRING) as planet_star_radios_ratio,
CAST(transit_properties.planet_star_radius_ratio.planet_star_radius_ratio_upper_unc AS STRING) as planet_star_radius_ratio_upper_unc,
CAST(transit_properties.planet_star_radius_ratio.planet_star_radius_ratio_lower_unc AS STRING) as planet_star_radius_ratio_lower_unc,
CAST(transit_properties.fitted_stellar_density.fitted_stellar_density AS STRING) as fitted_stellar_density,
CAST(transit_properties.fitted_stellar_density.fitted_stellar_density_upper_unc_gcm3 AS STRING) as fitted_stellar_density_upper_unc_gcm3,
CAST(transit_properties.fitted_stellar_density.fitted_stellar_density_lower_unc_gcm3 AS STRING) as fitted_stellar_density_lower_unc_gcm3,
CAST(transit_properties.planetary_fit_type AS STRING) as planetary_fit_type,
CAST(transit_properties.planetary_radius.planetary_radius_earth_radii AS STRING) as planetary_radius_earth_radii,
CAST(transit_properties.planetary_radius.planetary_radius_upper_unc_earth_raddi AS STRING) as planetary_radius_upper_unc_earth_raddi,
CAST(transit_properties.planetary_radius.planetary_radius_lower_unc_earth_raddi AS STRING) as planetary_radius_lower_unc_earth_raddi,
CAST(transit_properties.orbit_semi_major_axis.orbit_semi_major_axis_au AS STRING) as orbit_semi_major_axis_au,
CAST(transit_properties.orbit_semi_major_axis.orbit_semi_major_axis_upper_unc_au AS STRING) as orbit_semi_major_axis_upper_unc_au,
CAST(transit_properties.orbit_semi_major_axis.orbit_semi_major_axis_lower_unc_au AS STRING) as orbit_semi_major_axis_lower_unc_au,
CAST(transit_properties.inclination.inclination_deg AS STRING) as inclination_deg,
CAST(transit_properties.inclination.inclination_upper_unc_deg AS STRING) as inclination_upper_unc_deg,
CAST(transit_properties.inclination.inclination_lower_unc_deg AS STRING) as inclination_lower_unc_deg,
CAST(transit_properties.equilibrium_temperature.equilibrium_temperature_k AS STRING) as equilibrium_temperature_k,
CAST(transit_properties.equilibrium_temperature.equilibrium_temperature_upper_unc_k AS STRING) as equilibrium_temperature_upper_unc_k,
CAST(transit_properties.equilibrium_temperature.equilibrium_temperature_lower_unc_k AS STRING) as equilibrium_temperature_lower_unc_k,
CAST(transit_properties.isolotaion_flux.isolation_flux AS STRING) as isolation_flux,
CAST(transit_properties.isolotaion_flux.isolotaion_flux_upper_unc AS STRING) as isolotaion_flux_upper_unc,
CAST(transit_properties.isolotaion_flux.isolation_flux_lower_unc AS STRING) as isolation_flux_lower_unc,
CAST(transit_properties.planet_star_distance_over_star_radius.planet_star_distance_over_star_radius AS STRING) as planet_star_distance_over_star_radius,
CAST(transit_properties.planet_star_distance_over_star_radius.planet_star_distance_over_star_radius_upper AS STRING) as planet_star_distance_over_star_radius_upper,
CAST(transit_properties.planet_star_distance_over_star_radius.planet_star_distance_over_star_radius_lower AS STRING) as planet_star_distance_over_star_radius_lower,
CAST(transit_properties.limb_darkening_model AS STRING) as limb_darkening_model,
CAST(transit_properties.limb_darkening_coeff_4 AS STRING) as limb_darkening_coeff_4,
CAST(transit_properties.limb_darkening_coeff_3 AS STRING) as limb_darkening_coeff_3,
CAST(transit_properties.limb_darkening_coeff_2 AS STRING) as limb_darkening_coeff_2,
CAST(transit_properties.limb_darkening_coeff_1 AS STRING) as limb_darkening_coeff_1,
CAST(transit_properties.parameters_provenance AS STRING) as parameters_provenance,
CAST(threshold_corssing_event_information.maximum_single_event_statistic AS STRING) as maximum_single_event_statistic,
CAST(threshold_corssing_event_information.maximum_multiple_event_statistic AS STRING) as maximum_multiple_event_statistic,
CAST(threshold_corssing_event_information.transit_signal_to_noise AS STRING) as transit_signal_to_noise,
CAST(threshold_corssing_event_information.number_of_planets AS STRING) as number_of_planets,
CAST(threshold_corssing_event_information.number_of_transits AS STRING) as number_of_transits,
CAST(threshold_corssing_event_information.tce_planet_number AS STRING) as tce_planet_number,
CAST(threshold_corssing_event_information.tce_delivery AS STRING) as tce_delivery,
CAST(threshold_corssing_event_information.quarters AS STRING) as quarters,
CAST(threshold_corssing_event_information.odd_even_depth_comparision_statistic AS STRING) as odd_even_depth_comparision_statistic,
CAST(threshold_corssing_event_information.transit_model AS STRING) as transit_model,
CAST(threshold_corssing_event_information.degress_of_freedom AS STRING) as degress_of_freedom,
CAST(threshold_corssing_event_information.chi_square AS STRING) as chi_square,
CAST(threshold_corssing_event_information.link_to_dv_report AS STRING) as link_to_dv_report,
CAST(threshold_corssing_event_information.link_to_dv_summary AS STRING) as link_to_dv_summary,
CAST(stellar_paramenters.stellar_effective_temperature_k.stellar_effective_temperature_k AS STRING) as stellar_effective_temperature_k,
CAST(stellar_paramenters.stellar_effective_temperature_k.stellar_effective_temperature_upper_unc_k AS STRING) as stellar_effective_temperature_upper_unc_k,
CAST(stellar_paramenters.stellar_effective_temperature_k.stellar_effective_temperature_lower_unc_k AS STRING) as stellar_effective_temperature_lower_unc_k,
CAST(stellar_paramenters.stellar_surface_gravity.stellar_surface_gravity AS STRING) as stellar_surface_gravity,
CAST(stellar_paramenters.stellar_surface_gravity.stellar_surface_gravity_upper_unc AS STRING) as stellar_surface_gravity_upper_unc,
CAST(stellar_paramenters.stellar_surface_gravity.stellar_surface_gravity_lower_unc AS STRING) as stellar_surface_gravity_lower_unc,
CAST(stellar_paramenters.stellar_metallicity.stellar_metallicity_dex AS STRING) as stellar_metallicity_dex,
CAST(stellar_paramenters.stellar_metallicity.stellar_metallicity_upper_unc AS STRING) as stellar_metallicity_upper_unc,
CAST(stellar_paramenters.stellar_metallicity.stellar_metallicity_lower_unc AS STRING) as stellar_metallicity_lower_unc,
CAST(stellar_paramenters.stellar_radius.stellar_radius_solar_raddi AS STRING) as stellar_radius_solar_raddi,
CAST(stellar_paramenters.stellar_radius.stellar_radius_upper_unc_solar_raddi AS STRING) as stellar_radius_upper_unc_solar_raddi,
CAST(stellar_paramenters.stellar_radius.stellar_radius_lower_unc_solar_raddi AS STRING) as stellar_radius_lower_unc_solar_raddi,
CAST(stellar_paramenters.stellar_mass.stellar_mass_solar_mass AS STRING) as stellar_mass_solar_mass,
CAST(stellar_paramenters.stellar_mass.stellar_mass_upper_unc_solar_mass AS STRING) as stellar_mass_upper_unc_solar_mass,
CAST(stellar_paramenters.stellar_mass.stellar_mass_lower_unc_solar_mass AS STRING) as stellar_mass_lower_unc_solar_mass,
CAST(stellar_paramenters.stellar_age.stellar_age_gyr AS STRING) as stellar_age_gyr,
CAST(stellar_paramenters.stellar_age.stellar_age_upper_unc_gyr AS STRING) as stellar_age_upper_unc_gyr,
CAST(stellar_paramenters.stellar_age.stellar_age_lower_unc_gyr AS STRING) as stellar_age_lower_unc_gyr,
CAST(stellar_parameter_provenance AS STRING) as stellar_parameter_provenance,
CAST(kic_parameters.ra AS STRING) as ra,
CAST(kic_parameters.dec AS STRING) as dec,
CAST(kic_parameters.kepler_band AS STRING) as kepler_band,
CAST(kic_parameters.g_band AS STRING) as g_band,
CAST(kic_parameters.r_band AS STRING) as r_band,
CAST(kic_parameters.i_band AS STRING) as i_band,
CAST(kic_parameters.z_band AS STRING) as z_band,
CAST(kic_parameters.j_band AS STRING) as j_band,
CAST(kic_parameters.h_band AS STRING) as h_band,
CAST(kic_parameters.k_band AS STRING) as k_band,
date_format(current_date(), 'yyyyMMdd') as yearmonthday



FROM df""")



###########################LOAD###########################
nasa.write.mode("overwrite").format("parquet").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/BIGDATA/nasa/stagin/bronze/nasa/")
