#!/usr/local/bin/python3
#coding: utf-8
#VARIABLES

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: nasa
#     Repositorio: Json
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports
import json
import csv
from faker import Faker
import faker_commerce
import faker_microservice
from faker_vehicle import VehicleProvider
from faker_music import MusicProvider
import random
from datetime import date, datetime


#setup
faker = Faker()
fakerPT = Faker('pt-Br')
fakerFR = Faker('fr_FR')
fakerIT = Faker('it_IT')
fakerRR = Faker('sv_SE')
fakerNO = Faker('no_NO')
fakerJP = Faker('ja_JP')
fakerAF = Faker('id_ID')

class Variables():
    def __init__(self, val) -> None:
        num = 0
        for i in range(val):
            num = num + 1
            with open(f'C:/Users/Bates/Documents/Repositorios/BIGDATA/nasa/dataset/json_files/tables_{num}.json', "w") as output:
                keipID = random.randint(111111111,999999999)
                koi_name = f"K000{random.randint(111,999)}.0{random.randint(1,9)}"
                kepler_name = f"Kepler-{random.randint(1,2000)} {faker.text().lower()[0]}"
                exoplanet_disposition = random.choice(["FALSE POSITIVE"])
                vetting_status = "done"
                date_of_last_parameter_update = f"{faker.date_this_decade()}"
                disposition_using_kepler_data = random.choice(["FALSE POSITIVE", "NOT DISPOSITIONED"])
                disposition_score = random.choice([0.0,float(random.randint(1,9))])
                not_transit_like_false_positive_flag = random.choice([0,1])
                stellar_eclipse_false_positive_flag = random.choice([0,1])
                centroid_offset_false_positive = random.choice([0,1])
                centroid_offset_false_positive_flag = faker.pyfloat(positive=True)
                ephemeris_math_indicates_contamination_false_positive_flag = random.choice([0,1])
                disposition_provenance = f"q{random.randint(1,9)}_q{random.randint(10,90)}_dr{random.randint(10,99)}_tc{faker.text().lower()[0]}"
                orbital_period_days = faker.pyfloat(positive=True)
                orbital_period_upper_unc_days = f"{random.randint(1,9)}.{random.randint(1,9)}000-e0{random.randint(1,9)}"
                orbital_period_lower_unc_days = f"-{orbital_period_upper_unc_days}"
                transit_epoch_bkjd = faker.pyfloat(positive=True)
                transit_epoch_upper_unc_bkjd = f"{random.randint(1,9)}.{random.randint(1,9)}000-e0{random.randint(1,9)}"
                transit_epoch_lower_unc_bkjd = f"-{transit_epoch_upper_unc_bkjd}"
                transit_epoch_bjd = faker.pyfloat(positive=True)
                transit_epoch_upper_unc_bjd = f"{random.randint(1,9)}.{random.randint(1,9)}000-e0{random.randint(1,9)}"
                transit_epoch_lower_unc_bjd = f"-{transit_epoch_upper_unc_bjd}"
                eccentricity = random.choice([0,1])
                eccentricity_upper_unc = random.choice(['0','1'])
                eccentricity_lower_unc = f"-{eccentricity_upper_unc}"
                long_periastron_deg = eccentricity
                long_periastron_upper_unc_deg = eccentricity_upper_unc
                long_periastron_lower_unc_deg = eccentricity_lower_unc
                impact_parameter = faker.pyfloat(positive=True)
                impact_parameter_upper_unc = round(faker.pyfloat(left_digits=True,positive=True),1)
                impact_parameter_lower_unc = -impact_parameter_upper_unc
                transit_duration_hrs = faker.pyfloat(positive=True)
                transit_duration_upper_unc_hrs = round(faker.pyfloat(left_digits=True,positive=True),1)
                transit_duration_lower_unc_hrs = -transit_duration_upper_unc_hrs
                ingress_duration_hrs = faker.pyfloat(positive=True)
                ingress_duration_upper_unc = round(faker.pyfloat(left_digits=True,positive=True),1)
                ingress_duration_lower_unc = - ingress_duration_upper_unc
                transit_depth_ppm = faker.pyint(max_value=9999)
                transit_depth_upper_unc_ppm = faker.pyint(max_value=99)
                transit_depth_lower_unc_ppm = -transit_depth_upper_unc_ppm
                planet_star_radios_ratio = faker.pyfloat(positive=True)
                planet_star_radius_ratio_upper_unc = f"{random.randint(1,9)}.{random.randint(1,9)}000-e0{random.randint(1,9)}"
                planet_star_radius_ratio_lower_unc = f"-{planet_star_radius_ratio_upper_unc}"
                fitted_stellar_density = faker.pyfloat(positive=True)
                fitted_stellar_density_upper_unc_gcm3 = f"{random.randint(1,9)}.{random.randint(1,9)}000-e0{random.randint(1,9)}"
                fitted_stellar_density_lower_unc_gcm3 = f"-{fitted_stellar_density_upper_unc_gcm3}"
                planetary_fit_type = faker.pyfloat(positive=True)
                planetary_radius_earth_radii = faker.pyfloat(positive=True)
                planetary_radius_upper_unc_earth_raddi = faker.pyfloat(positive=True)
                planetary_radius_lower_unc_earth_raddi = -planetary_radius_upper_unc_earth_raddi
                orbit_semi_major_axis_au = faker.pyfloat(positive=True)
                orbit_semi_major_axis_upper_unc_au = faker.pyfloat(positive=True)
                orbit_semi_major_axis_lower_unc_au = - orbit_semi_major_axis_upper_unc_au
                inclination_deg = faker.pyfloat(positive=True)
                inclination_upper_unc_deg = faker.pyfloat(positive=True)
                inclination_lower_unc_deg = - inclination_upper_unc_deg
                equilibrium_temperature_k = faker.pyfloat(positive=True)
                equilibrium_temperature_upper_unc_k =  faker.pyfloat(positive=True)
                equilibrium_temperature_lower_unc_k = -equilibrium_temperature_upper_unc_k
                isolation_flux = faker.pyfloat(positive=True)
                isolotaion_flux_upper_unc = faker.pyfloat(positive=True)
                isolation_flux_lower_unc = -isolotaion_flux_upper_unc
                planet_star_distance_over_star_radius = faker.pyfloat(positive=True)
                planet_star_distance_over_star_radius_upper = f"{random.randint(1,9)}.{random.randint(1,9)}000-e0{random.randint(1,9)}"
                planet_star_distance_over_star_radius_lower = f"-{planet_star_distance_over_star_radius_upper}"
                limb_darkening_model = f"""Claret({random.randint(2001,2021)} {random.choice(["A", "B", "C", "D", "Q", "E", "P"])}&{random.choice(["A", "B", "C", "D", "Q", "E", "P"])} {random.randint(100,999)} {random.randint(10,99)}) ATLAS {faker.text().upper()[0]}{faker.text().upper()[0]}"""
                limb_darkening_coeff_4 = faker.pyfloat(positive=True)
                limb_darkening_coeff_3 = faker.pyfloat(positive=True)
                limb_darkening_coeff_2 = faker.pyfloat(positive=True)
                limb_darkening_coeff_1 = faker.pyfloat(positive=True)
                parameters_provenance = f"q{random.randint(1,9)}_q{random.randint(10,99)}_{random.choice(['koi','kepler'])}"
                maximum_single_event_statistic = faker.pyfloat(positive=True)
                maximum_multiple_event_statistic =faker.pyfloat(positive=True)
                transit_signal_to_noise = faker.pyfloat(positive=True)
                number_of_planets = random.randint(1,100)
                number_of_transits = random.randint(1,10)
                tce_planet_number = random.randint(1,9)
                tce_delivery = f"q{random.randint(1,9)}_q{random.randint(10,99)}_tce"
                quarters = faker.pyfloat(positive=True)
                odd_even_depth_comparision_statistic = faker.pyfloat(positive=True)
                transit_model = f"Mandel and Agol ({random.randint(2000,2020)} {faker.text()[0]}{faker.text().lower()[0]}{faker.text()[0]} {random.randint(100,999)} {random.randint(100,900)})"
                degress_of_freedom = faker.pyfloat(positive=True)
                chi_square = faker.pyfloat(positive=True)
                link_to_dv_report = faker.image_url()
                link_to_dv_summary = faker.image_url()
                stellar_effective_temperature_k = faker.pyfloat(positive=True)
                stellar_effective_temperature_upper_unc_k = faker.pyfloat(positive=True)
                stellar_effective_temperature_lower_unc_k = faker.pyfloat(positive=True)
                stellar_surface_gravity = faker.pyfloat(positive=True)
                stellar_surface_gravity_upper_unc = faker.pyfloat(positive=True)
                stellar_surface_gravity_lower_unc = faker.pyfloat(positive=True)
                stellar_metallicity_dex = faker.pyfloat()
                stellar_metallicity_upper_unc = faker.pyfloat()
                stellar_metallicity_lower_unc = faker.pyfloat()
                stellar_radius_solar_raddi = faker.pyfloat(positive=True)
                stellar_radius_upper_unc_solar_raddi = faker.pyfloat(positive=True)
                stellar_radius_lower_unc_solar_raddi = faker.pyfloat(positive=True)
                stellar_mass_solar_mass = faker.pyfloat(positive=True)
                stellar_mass_upper_unc_solar_mass = faker.pyfloat(positive=True)
                stellar_mass_lower_unc_solar_mass  = faker.pyfloat(positive=True)
                stellar_age_gyr = faker.pyfloat(positive=True)
                stellar_age_upper_unc_gyr = faker.pyfloat(positive=True)
                stellar_age_lower_unc_gyr = faker.pyfloat(positive=True)
                stellar_parameter_provenance = f"stellar_q{random.randint(1,9)}_q{random.randint(10,99)}"
                ra = faker.pyfloat(positive=True)
                dec = faker.pyfloat(positive=True)
                kepler_band = faker.pyfloat(positive=True)
                g_band = faker.pyfloat(positive=True)
                r_band = faker.pyfloat(positive=True)
                i_band = faker.pyfloat(positive=True)
                z_band = faker.pyfloat(positive=True)
                j_band =  faker.pyfloat(positive=True)
                h_band = faker.pyfloat(positive=True)
                k_band = faker.pyfloat(positive=True)
                
                
                if not_transit_like_false_positive_flag == 0 and stellar_eclipse_false_positive_flag == 0 and centroid_offset_false_positive == 0 and ephemeris_math_indicates_contamination_false_positive_flag == 0:
                    disposition_using_kepler_data = "CANDIDATE"
                    exoplanet_disposition = "CONFIRMED"
                    tab = {
                        "identification": {
                            "keipID": keipID,
                            "koi_name": koi_name
                        },
                        "exoplanet_information": {
                            "kepler_name":kepler_name ,
                            "exoplanet_disposition": exoplanet_disposition ,
                            "vetting_status": vetting_status,
                            "date_of_last_parameter_update": date_of_last_parameter_update,
                        },
                        "project_disposition": {
                            "disposition_using_kepler_data": disposition_using_kepler_data,
                            "disposition_score": disposition_score,
                            "not_transit_like_false_positive_flag": not_transit_like_false_positive_flag,
                            "stellar_eclipse_false_positive_flag": stellar_eclipse_false_positive_flag,
                            "centroid_offset_false_positive_flag": centroid_offset_false_positive_flag,
                            "ephemeris_math_indicates_contamination_false_positive_flag": ephemeris_math_indicates_contamination_false_positive_flag,
                            "disposition_provenance":  disposition_provenance
                        },
                        "transit_properties": {
                            "orbital_period": {
                                "orbital_period_days": orbital_period_days,
                                "orbital_period_upper_unc_days": orbital_period_upper_unc_days,
                                "orbital_period_lower_unc_days": orbital_period_lower_unc_days ,
                            },
                            "transit_epoch_bkjd": {
                                "transit_epoch_bkjd": transit_epoch_bkjd,
                                "transit_epoch_upper_unc_bkjd": transit_epoch_upper_unc_bkjd,
                                "transit_epoch_lower_unc_bkjd": transit_epoch_lower_unc_bkjd
                            },
                            "transit_epoch_bjd": {
                                "transit_epoch_bjd": transit_epoch_bjd,
                                "transit_epoch_upper_unc_bjd": transit_epoch_upper_unc_bjd,
                                "transit_epoch_lower_unc_bjd": transit_epoch_lower_unc_bjd
                            },
                            "eccentricity": {
                                "eccentricity": eccentricity,
                                "eccentricity_upper_unc":eccentricity_upper_unc ,
                                "eccentricity_lower_unc": eccentricity_lower_unc,
                            },
                            "long_periastron": {
                                "long_periastron_deg": long_periastron_deg,
                                "long_periastron_upper_unc_deg": long_periastron_upper_unc_deg,
                                "long_periastron_lower_unc_deg": long_periastron_lower_unc_deg
                            },
                            "impact_parameter": {
                                "impact_parameter": impact_parameter,
                                "impact_parameter_upper_unc": impact_parameter_upper_unc,
                                "impact_parameter_lower_unc": impact_parameter_lower_unc
                            },
                            "transit_duration": {
                                "transit_duration_hrs": transit_duration_hrs,
                                "transit_duration_upper_unc_hrs": transit_duration_upper_unc_hrs,
                                "transit_duration_lower_unc_hrs": transit_duration_lower_unc_hrs
                            },
                            "ingress_duration": {
                                "ingress_duration_hrs": ingress_duration_hrs,
                                "ingress_duration_upper_unc": ingress_duration_upper_unc,
                                "ingress_duration_lower_unc": ingress_duration_lower_unc
                            },
                            "transit_depth": {
                                "transit_depth_ppm":transit_depth_ppm ,
                                "transit_depth_upper_unc_ppm":transit_depth_upper_unc_ppm ,
                                "transit_depth_lower_unc_ppm": transit_depth_lower_unc_ppm
                            },
                            "planet_star_radius_ratio": {
                                "planet_star_radios_ratio": planet_star_radios_ratio,
                                "planet_star_radius_ratio_upper_unc":planet_star_radius_ratio_upper_unc,
                                "planet_star_radius_ratio_lower_unc":planet_star_radius_ratio_lower_unc
                            },
                            "fitted_stellar_density": {
                                "fitted_stellar_density": fitted_stellar_density,
                                "fitted_stellar_density_upper_unc_gcm3":fitted_stellar_density_upper_unc_gcm3 ,
                                "fitted_stellar_density_lower_unc_gcm3": fitted_stellar_density_lower_unc_gcm3
                            },
                            "planetary_fit_type": planetary_fit_type,
                            "planetary_radius": {
                                "planetary_radius_earth_radii": planetary_radius_earth_radii,
                                "planetary_radius_upper_unc_earth_raddi": planetary_radius_upper_unc_earth_raddi,
                                "planetary_radius_lower_unc_earth_raddi": planetary_radius_lower_unc_earth_raddi
                            },
                            "orbit_semi_major_axis": {
                                "orbit_semi_major_axis_au": orbit_semi_major_axis_au,
                                "orbit_semi_major_axis_upper_unc_au": orbit_semi_major_axis_upper_unc_au,
                                "orbit_semi_major_axis_lower_unc_au": orbit_semi_major_axis_lower_unc_au
                            },
                            "inclination": {
                                "inclination_deg": inclination_deg,
                                "inclination_upper_unc_deg": inclination_upper_unc_deg,
                                "inclination_lower_unc_deg": inclination_lower_unc_deg
                            },
                            "equilibrium_temperature": {
                                "equilibrium_temperature_k": equilibrium_temperature_k,
                                "equilibrium_temperature_upper_unc_k":equilibrium_temperature_upper_unc_k ,
                                "equilibrium_temperature_lower_unc_k": equilibrium_temperature_lower_unc_k
                            },
                            "isolotaion_flux": {
                                "isolation_flux": isolation_flux,
                                "isolotaion_flux_upper_unc":isolotaion_flux_upper_unc ,
                                "isolation_flux_lower_unc": isolation_flux_lower_unc
                            },
                            "planet_star_distance_over_star_radius": {
                                "planet_star_distance_over_star_radius": planet_star_distance_over_star_radius,
                                "planet_star_distance_over_star_radius_upper": planet_star_distance_over_star_radius_upper,
                                "planet_star_distance_over_star_radius_lower": planet_star_distance_over_star_radius_lower
                            },
                            "limb_darkening_model": limb_darkening_model ,
                            "limb_darkening_coeff_4":limb_darkening_coeff_4 ,
                            "limb_darkening_coeff_3": limb_darkening_coeff_3,
                            "limb_darkening_coeff_2":limb_darkening_coeff_2 ,
                            "limb_darkening_coeff_1": limb_darkening_coeff_1,
                            "parameters_provenance": parameters_provenance
                        },
                        "threshold_corssing_event_information": {
                            "maximum_single_event_statistic": maximum_single_event_statistic,
                            "maximum_multiple_event_statistic": maximum_multiple_event_statistic,
                            "transit_signal_to_noise": transit_signal_to_noise,
                            "number_of_planets": number_of_planets,
                            "number_of_transits":number_of_transits ,
                            "tce_planet_number": tce_planet_number,
                            "tce_delivery":tce_delivery ,
                            "quarters": quarters,
                            "odd_even_depth_comparision_statistic":odd_even_depth_comparision_statistic ,
                            "transit_model": transit_model,
                            "degress_of_freedom":degress_of_freedom,
                            "chi_square": chi_square,
                            "link_to_dv_report":link_to_dv_report,
                            "link_to_dv_summary":link_to_dv_summary
                        },
                        "stellar_paramenters": {
                            "stellar_effective_temperature_k": {
                                "stellar_effective_temperature_k": stellar_effective_temperature_k,
                                "stellar_effective_temperature_upper_unc_k": stellar_effective_temperature_upper_unc_k,
                                "stellar_effective_temperature_lower_unc_k": stellar_effective_temperature_lower_unc_k
                            },
                            "stellar_surface_gravity": {
                                "stellar_surface_gravity": stellar_surface_gravity,
                                "stellar_surface_gravity_upper_unc": stellar_surface_gravity_upper_unc,
                                "stellar_surface_gravity_lower_unc": stellar_surface_gravity_lower_unc
                            },
                            "stellar_metallicity": {
                                "stellar_metallicity_dex": stellar_metallicity_dex,
                                "stellar_metallicity_upper_unc": stellar_metallicity_upper_unc,
                                "stellar_metallicity_lower_unc": stellar_metallicity_lower_unc
                            },
                            "stellar_radius": {
                                "stellar_radius_solar_raddi":stellar_radius_solar_raddi ,
                                "stellar_radius_upper_unc_solar_raddi":stellar_radius_upper_unc_solar_raddi ,
                                "stellar_radius_lower_unc_solar_raddi": stellar_radius_lower_unc_solar_raddi
                            },
                            "stellar_mass": {
                                "stellar_mass_solar_mass": stellar_mass_solar_mass,
                                "stellar_mass_upper_unc_solar_mass":stellar_mass_upper_unc_solar_mass,
                                "stellar_mass_lower_unc_solar_mass":stellar_mass_lower_unc_solar_mass
                            },
                            "stellar_age": {
                                "stellar_age_gyr":stellar_age_gyr ,
                                "stellar_age_upper_unc_gyr": stellar_age_upper_unc_gyr,
                                "stellar_age_lower_unc_gyr":stellar_age_lower_unc_gyr
                            }
                        },
                        "stellar_parameter_provenance": stellar_parameter_provenance,
                        "kic_parameters": {
                        "ra": ra,
                        "dec": dec,
                        "kepler_band": kepler_band,
                        "g_band": g_band,
                        "r_band": r_band,
                        "i_band": i_band,
                        "z_band": z_band,
                        "j_band": j_band,
                        "h_band": h_band,
                        "k_band": k_band
                        }}
                        


                    json.dump(tab, output, allow_nan=True, indent=True, separators=(',',':'))
                else:
                    tab = {
                        "identification": {
                            "keipID": keipID,
                            "koi_name": koi_name
                        },
                        "exoplanet_information": {
                            "kepler_name":kepler_name ,
                            "exoplanet_disposition": exoplanet_disposition ,
                            "vetting_status": vetting_status,
                            "date_of_last_parameter_update": date_of_last_parameter_update,
                        },
                        "project_disposition": {
                            "disposition_using_kepler_data": disposition_using_kepler_data,
                            "disposition_score": disposition_score,
                            "not_transit_like_false_positive_flag": not_transit_like_false_positive_flag,
                            "stellar_eclipse_false_positive_flag": stellar_eclipse_false_positive_flag,
                            "centroid_offset_false_positive_flag": centroid_offset_false_positive_flag,
                            "ephemeris_math_indicates_contamination_false_positive_flag": ephemeris_math_indicates_contamination_false_positive_flag,
                            "disposition_provenance":  disposition_provenance
                        },
                        "transit_properties": {
                            "orbital_period": {
                                "orbital_period_days": orbital_period_days,
                                "orbital_period_upper_unc_days": orbital_period_upper_unc_days,
                                "orbital_period_lower_unc_days": orbital_period_lower_unc_days ,
                            },
                            "transit_epoch_bkjd": {
                                "transit_epoch_bkjd": transit_epoch_bkjd,
                                "transit_epoch_upper_unc_bkjd": transit_epoch_upper_unc_bkjd,
                                "transit_epoch_lower_unc_bkjd": transit_epoch_lower_unc_bkjd
                            },
                            "transit_epoch_bjd": {
                                "transit_epoch_bjd": transit_epoch_bjd,
                                "transit_epoch_upper_unc_bjd": transit_epoch_upper_unc_bjd,
                                "transit_epoch_lower_unc_bjd": transit_epoch_lower_unc_bjd
                            },
                            "eccentricity": {
                                "eccentricity": eccentricity,
                                "eccentricity_upper_unc":eccentricity_upper_unc ,
                                "eccentricity_lower_unc": eccentricity_lower_unc,
                            },
                            "long_periastron": {
                                "long_periastron_deg": long_periastron_deg,
                                "long_periastron_upper_unc_deg": long_periastron_upper_unc_deg,
                                "long_periastron_lower_unc_deg": long_periastron_lower_unc_deg
                            },
                            "impact_parameter": {
                                "impact_parameter": impact_parameter,
                                "impact_parameter_upper_unc": impact_parameter_upper_unc,
                                "impact_parameter_lower_unc": impact_parameter_lower_unc
                            },
                            "transit_duration": {
                                "transit_duration_hrs": transit_duration_hrs,
                                "transit_duration_upper_unc_hrs": transit_duration_upper_unc_hrs,
                                "transit_duration_lower_unc_hrs": transit_duration_lower_unc_hrs
                            },
                            "ingress_duration": {
                                "ingress_duration_hrs": ingress_duration_hrs,
                                "ingress_duration_upper_unc": ingress_duration_upper_unc,
                                "ingress_duration_lower_unc": ingress_duration_lower_unc
                            },
                            "transit_depth": {
                                "transit_depth_ppm":transit_depth_ppm ,
                                "transit_depth_upper_unc_ppm":transit_depth_upper_unc_ppm ,
                                "transit_depth_lower_unc_ppm": transit_depth_lower_unc_ppm
                            },
                            "planet_star_radius_ratio": {
                                "planet_star_radios_ratio": planet_star_radios_ratio,
                                "planet_star_radius_ratio_upper_unc":planet_star_radius_ratio_upper_unc,
                                "planet_star_radius_ratio_lower_unc":planet_star_radius_ratio_lower_unc
                            },
                            "fitted_stellar_density": {
                                "fitted_stellar_density": fitted_stellar_density,
                                "fitted_stellar_density_upper_unc_gcm3":fitted_stellar_density_upper_unc_gcm3 ,
                                "fitted_stellar_density_lower_unc_gcm3": fitted_stellar_density_lower_unc_gcm3
                            },
                            "planetary_fit_type": planetary_fit_type,
                            "planetary_radius": {
                                "planetary_radius_earth_radii": planetary_radius_earth_radii,
                                "planetary_radius_upper_unc_earth_raddi": planetary_radius_upper_unc_earth_raddi,
                                "planetary_radius_lower_unc_earth_raddi": planetary_radius_lower_unc_earth_raddi
                            },
                            "orbit_semi_major_axis": {
                                "orbit_semi_major_axis_au": orbit_semi_major_axis_au,
                                "orbit_semi_major_axis_upper_unc_au": orbit_semi_major_axis_upper_unc_au,
                                "orbit_semi_major_axis_lower_unc_au": orbit_semi_major_axis_lower_unc_au
                            },
                            "inclination": {
                                "inclination_deg": inclination_deg,
                                "inclination_upper_unc_deg": inclination_upper_unc_deg,
                                "inclination_lower_unc_deg": inclination_lower_unc_deg
                            },
                            "equilibrium_temperature": {
                                "equilibrium_temperature_k": equilibrium_temperature_k,
                                "equilibrium_temperature_upper_unc_k":equilibrium_temperature_upper_unc_k ,
                                "equilibrium_temperature_lower_unc_k": equilibrium_temperature_lower_unc_k
                            },
                            "isolotaion_flux": {
                                "isolation_flux": isolation_flux,
                                "isolotaion_flux_upper_unc":isolotaion_flux_upper_unc ,
                                "isolation_flux_lower_unc": isolation_flux_lower_unc
                            },
                            "planet_star_distance_over_star_radius": {
                                "planet_star_distance_over_star_radius": planet_star_distance_over_star_radius,
                                "planet_star_distance_over_star_radius_upper": planet_star_distance_over_star_radius_upper,
                                "planet_star_distance_over_star_radius_lower": planet_star_distance_over_star_radius_lower
                            },
                            "limb_darkening_model": limb_darkening_model ,
                            "limb_darkening_coeff_4":limb_darkening_coeff_4 ,
                            "limb_darkening_coeff_3": limb_darkening_coeff_3,
                            "limb_darkening_coeff_2":limb_darkening_coeff_2 ,
                            "limb_darkening_coeff_1": limb_darkening_coeff_1,
                            "parameters_provenance": parameters_provenance
                        },
                        "threshold_corssing_event_information": {
                            "maximum_single_event_statistic": maximum_single_event_statistic,
                            "maximum_multiple_event_statistic": maximum_multiple_event_statistic,
                            "transit_signal_to_noise": transit_signal_to_noise,
                            "number_of_planets": number_of_planets,
                            "number_of_transits":number_of_transits ,
                            "tce_planet_number": tce_planet_number,
                            "tce_delivery":tce_delivery ,
                            "quarters": quarters,
                            "odd_even_depth_comparision_statistic":odd_even_depth_comparision_statistic ,
                            "transit_model": transit_model,
                            "degress_of_freedom":degress_of_freedom,
                            "chi_square": chi_square,
                            "link_to_dv_report":link_to_dv_report,
                            "link_to_dv_summary":link_to_dv_summary
                        },
                        "stellar_paramenters": {
                            "stellar_effective_temperature_k": {
                                "stellar_effective_temperature_k": stellar_effective_temperature_k,
                                "stellar_effective_temperature_upper_unc_k": stellar_effective_temperature_upper_unc_k,
                                "stellar_effective_temperature_lower_unc_k": stellar_effective_temperature_lower_unc_k
                            },
                            "stellar_surface_gravity": {
                                "stellar_surface_gravity": stellar_surface_gravity,
                                "stellar_surface_gravity_upper_unc": stellar_surface_gravity_upper_unc,
                                "stellar_surface_gravity_lower_unc": stellar_surface_gravity_lower_unc
                            },
                            "stellar_metallicity": {
                                "stellar_metallicity_dex": stellar_metallicity_dex,
                                "stellar_metallicity_upper_unc": stellar_metallicity_upper_unc,
                                "stellar_metallicity_lower_unc": stellar_metallicity_lower_unc
                            },
                            "stellar_radius": {
                                "stellar_radius_solar_raddi":stellar_radius_solar_raddi ,
                                "stellar_radius_upper_unc_solar_raddi":stellar_radius_upper_unc_solar_raddi ,
                                "stellar_radius_lower_unc_solar_raddi": stellar_radius_lower_unc_solar_raddi
                            },
                            "stellar_mass": {
                                "stellar_mass_solar_mass": stellar_mass_solar_mass,
                                "stellar_mass_upper_unc_solar_mass":stellar_mass_upper_unc_solar_mass,
                                "stellar_mass_lower_unc_solar_mass":stellar_mass_lower_unc_solar_mass
                            },
                            "stellar_age": {
                                "stellar_age_gyr":stellar_age_gyr ,
                                "stellar_age_upper_unc_gyr": stellar_age_upper_unc_gyr,
                                "stellar_age_lower_unc_gyr":stellar_age_lower_unc_gyr
                            }
                        },
                        "stellar_parameter_provenance": stellar_parameter_provenance,
                        "kic_parameters": {
                        "ra": ra,
                        "dec": dec,
                        "kepler_band": kepler_band,
                        "g_band": g_band,
                        "r_band": r_band,
                        "i_band": i_band,
                        "z_band": z_band,
                        "j_band": j_band,
                        "h_band": h_band,
                        "k_band": k_band
                        }}
                        


                    json.dump(tab, output, allow_nan=True, indent=True, separators=(',',':'))



                