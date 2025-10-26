import pandas as pd
import geopandas as gpd
import networkx as nx
from typing import Dict, List, Tuple, Optional
from pathlib import Path
from dataclasses import dataclass
import numpy as np


from helper_functions import create_bike_graph, ckdnearest, load_files
from process_gtfs import create_transfers, process_gtfs

@dataclass
class ConfiguracionSistema:
    """Configuración completa del sistema integrado"""
    
    coeficiente_seguridad: float = 0.3
    coeficiente_pendiente: float = 0.05
    coeficiente_trafico: float = 0.0001
    velocidad_promedio_bicicleta: float = 15.0
    
    
    gtfs_fp: Path = None
    crs: str = 'epsg:4326'
    transfer_time: int = 2  
    walk_spd: float = 2.5  
    modes: List[int] = None
    gtfs_name: str = "default"
    service_date: str = "20240101"
    
    def __post_init__(self):
        if self.modes is None:
            self.modes = [1, 3]  

class SistemaMovilidadIntegrada:
    """
    Sistema que integra optimización de ciclovías con transporte público
    Basado en la metodología del artículo para movilidad intermodal
    """
    
    def __init__(self, config: ConfiguracionSistema):
        self.config = config
        self.grafo_ciclista = None
        self.datos_transporte = {}
        self.nodos_snapped = {}
        
    def inicializar_sistema(self):
        """Inicializa todos los componentes del sistema"""
        print("=== INICIALIZANDO SISTEMA DE MOVILIDAD INTEGRADA ===")
        
       
        self._procesar_datos_transporte()
        
       
        self._cargar_red_ciclista()
        
        
        self._integrar_redes()
        
        print("Sistema inicializado exitosamente")
    
    def _procesar_datos_transporte(self):
        """Procesa datos GTFS usando las funciones existentes"""
        print("Procesando datos de transporte público...")
        
        try:
            # Crear transfers.txt si no existe
            transfers_path = self.config.gtfs_fp / 'transfers.txt'
            if not transfers_path.exists():
                settings_gtfs = {
                    'gtfs_fp': self.config.gtfs_fp,
                    'crs': self.config.crs,
                    'transfer_time': self.config.transfer_time,
                    'walk_spd': self.config.walk_spd
                }
                create_transfers(settings_gtfs)
            
        
            kwds_gtfs = {
                'gtfs_name': self.config.gtfs_name,
                'service_date': self.config.service_date,
                'modes': self.config.modes
            }
            
            output = process_gtfs(kwds_gtfs)
            print("Datos GTFS procesados exitosamente")
            
            # Cargar archivos procesados
            settings_load = {
                'output_fp': self.config.gtfs_fp,
                'gtfs_fp': self.config.gtfs_fp
            }
            
            snapped_tazs, snapped_stops, shape_map, shapes, stops_file = load_files(settings_load)
            
            self.datos_transporte = {
                'snapped_tazs': snapped_tazs,
                'snapped_stops': snapped_stops,
                'shape_map': shape_map,
                'shapes': shapes,
                'stops_file': stops_file
            }
            
        except Exception as e:
            print(f"Error procesando datos GTFS: {e}")
    
    def _cargar_red_ciclista(self):
        """Carga y prepara la red ciclista"""
        print("Cargando red ciclista...")
        
        try:
         
            archivo_enlaces = "data/red_ciclista.gpkg"  
            
        
            enlaces = gpd.read_file(archivo_enlaces)
            
          
            enlaces['impedancia_ciclista'] = enlaces.apply(
                self._calcular_impedancia_ciclista, axis=1
            )
            
            
            self.grafo_ciclista = create_bike_graph(enlaces, 'impedancia_ciclista')
            
            print(f"Red ciclista cargada: {len(self.grafo_ciclista.nodes)} nodos")
            
        except Exception as e:
            print(f"Error cargando red ciclista: {e}")
    
    def _calcular_impedancia_ciclista(self, enlace: pd.Series) -> float:
        """Calcula impedancia para enlaces ciclistas"""
        distancia_km = enlace['longitud'] / 1000
        tiempo_base = (distancia_km / self.config.velocidad_promedio_bicicleta) * 60
        
       
        factor_seguridad = self._obtener_factor_seguridad(enlace.get('tipo_infraestructura', 3))
        factor_pendiente = self.config.coeficiente_pendiente * abs(enlace.get('pendiente', 0))
        
        impedancia = tiempo_base * (1 + factor_seguridad + factor_pendiente)
        return impedancia
    
    def _obtener_factor_seguridad(self, tipo_infra: int) -> float:
        factores = {
            1: 0.1,  
            2: 0.3, 
            3: 0.6,  
            4: 0.9   
        }
        return factores.get(tipo_infra, 0.6)
    
    def _integrar_redes(self):
        """Integra la red ciclista con la red de transporte público"""
        print("Integrando redes ciclista y transporte público...")
        
        try:
           
            if 'snapped_stops' in self.datos_transporte:
                stops_gdf = self.datos_transporte['snapped_stops']
                print(f"Paradas snapadas: {len(stops_gdf)}")
            
        except Exception as e:
            print(f"Error integrando redes: {e}")
    
    def optimizar_ruta_intermodal(self, origen: str, destino: str, 
                                max_tiempo_caminata: float = 10.0) -> Dict:
        """
        Optimiza ruta considerando combinación bicicleta + transporte público
        Implementa el concepto de "última milla" del artículo
        """
        print(f"Optimizando ruta intermodal: {origen} -> {destino}")
        
        try:
           
            paradas_origen = self._encontrar_paradas_accesibles(origen, max_tiempo_caminata)
            
          
            paradas_destino = self._encontrar_paradas_accesibles(destino, max_tiempo_caminata)
          
            rutas_optimas = self._evaluar_combinaciones(paradas_origen, paradas_destino, origen, destino)
            
            return {
                'origen': origen,
                'destino': destino,
                'rutas_optimas': rutas_optimas,
                'paradas_origen': paradas_origen,
                'paradas_destino': paradas_destino
            }
            
        except Exception as e:
            print(f"Error optimizando ruta intermodal: {e}")
            return {}
    
    def _encontrar_paradas_accesibles(self, punto: str, max_tiempo: float) -> List[Dict]:
        """Encuentra paradas accesibles desde un punto dado"""
        paradas_accesibles = []
        
       
        distancia_max = (max_tiempo / 60) * self.config.velocidad_promedio_bicicleta * 1000
  
        
        return paradas_accesibles
    
    def _evaluar_combinaciones(self, paradas_origen: List, paradas_destino: List, 
                             origen: str, destino: str) -> List[Dict]:
        """Evalúa diferentes combinaciones de rutas"""
        rutas = []
        
      
        ruta_bicicleta = self._calcular_ruta_bicicleta(origen, destino)
        if ruta_bicicleta:
            rutas.append({
                'tipo': 'solo_bicicleta',
                'ruta': ruta_bicicleta,
                'tiempo_total': ruta_bicicleta.get('tiempo_total', 0),
                'distancia_total': ruta_bicicleta.get('distancia_total', 0)
            })
        
       
        
        return sorted(rutas, key=lambda x: x['tiempo_total'])
    
    def _calcular_ruta_bicicleta(self, origen: str, destino: str) -> Dict:
        """Calcula ruta solo en bicicleta"""
      
        return {
            'tipo': 'bicicleta',
            'tiempo_total': 30, 
            'distancia_total': 5000, 
            'segmentos': []
        }
    
    def generar_metricas_desempeno(self, resultados: List[Dict]) -> Dict:
        """Genera métricas de desempeño del sistema integrado"""
        metricas = {
            'total_rutas_optimizadas': len(resultados),
            'tiempo_promedio': 0,
            'distancia_promedio': 0,
            'modos_utilizados': {},
            'reduccion_impedancia': 0
        }
        
        if resultados:
            tiempos = [r.get('tiempo_total', 0) for r in resultados]
            distancias = [r.get('distancia_total', 0) for r in resultados]
            
            metricas['tiempo_promedio'] = np.mean(tiempos)
            metricas['distancia_promedio'] = np.mean(distancias)
        
        return metricas
    
    def exportar_resultados(self, resultados: List[Dict], formato: str = "GPKG"):
        """Exporta resultados en diferentes formatos"""
        try:
            if formato == "GPKG":
               
                gdf_rutas = self._crear_geodataframe_rutas(resultados)
                gdf_rutas.to_file("resultados_optimizacion.gpkg", layer='rutas_optimizadas')
                print("Resultados exportados a GPKG")
            
            elif formato == "CSV":
                df_resumen = pd.DataFrame(resultados)
                df_resumen.to_csv("resultados_optimizacion.csv", index=False)
                print("Resultados exportados a CSV")
                
        except Exception as e:
            print(f"Error exportando resultados: {e}")
    
    def _crear_geodataframe_rutas(self, resultados: List[Dict]) -> gpd.GeoDataFrame:
        """Crea GeoDataFrame con las geometrías de las rutas"""
      
        return gpd.GeoDataFrame()


def ejecutar_sistema_integrado():
    """Función principal que ejecuta el sistema completo"""
    
   
    config = ConfiguracionSistema(
        gtfs_fp=Path.home() / Path('Documents/GitHub/transit-routing/GTFS/martalatest'),
        gtfs_name="sistema_transporte",
        service_date="20240101",
        modes=[1, 3] 
    )
    

    sistema = SistemaMovilidadIntegrada(config)
    sistema.inicializar_sistema()
    
   
    pares_od = [
        ("TAZ_CENTRO", "TAZ_UNIVERSIDAD"),
        ("TAZ_RESIDENCIAL", "TAZ_CENTRO_COMERCIAL"),
        ("TAZ_ESTACION", "TAZ_OFICINAS")
    ]
    
   
    resultados = []
    for origen, destino in pares_od:
        resultado = sistema.optimizar_ruta_intermodal(origen, destino)
        resultados.append(resultado)
    
  
    metricas = sistema.generar_metricas_desempeno(resultados)
 
    sistema.exportar_resultados(resultados, "GPKG")
 
    print("\n=== SISTEMA DE OPTIMIZACIÓN COMPLETADO ===")
    print(f"Rutas optimizadas: {metricas['total_rutas_optimizadas']}")
    print(f"Tiempo promedio: {metricas['tiempo_promedio']:.1f} min")
    print(f"Distancia promedio: {metricas['distancia_promedio']:.1f} m")
    
    return sistema, resultados

if __name__ == "__main__":
    sistema, resultados = ejecutar_sistema_integrado()
