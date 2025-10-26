

from pathlib import Path
import geopandas as gpd
import pandas as pd
import numpy as np
import pickle
from tqdm import tqdm
from shapely.ops import LineString
from datetime import datetime, timedelta, date
from typing import Dict, List, Tuple, Optional


from helper_functions import check_type, process_results, load_files

class MapeadorRutasCiclovias:
    """
    Sistema de mapeo especializado para rutas optimizadas de ciclovías
    Implementa la visualización de resultados del sistema SPCCM
    """
    
    def __init__(self, config: Dict):
        self.config = config
        self.rutas_mapeadas = []
        
    def generar_geometrias_rutas_optimizadas(self, resultados_optimizacion: List[Dict], 
                                           red_ciclovias: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Genera geometrías GeoDataFrame para las rutas optimizadas
        Basado en la metodología de reconstrucción de rutas del artículo
        """
        print("=== GENERANDO GEOMETRÍAS DE RUTAS OPTIMIZADAS ===")
        
        rutas_geometricas = []
        
        for resultado in tqdm(resultados_optimizacion, desc="Mapeando rutas"):
            try:
                ruta_geometrica = self._reconstruir_geometria_ruta(
                    resultado['ruta_optima'], 
                    red_ciclovias,
                    resultado
                )
                
                if ruta_geometrica:
                    ruta_mapeada = {
                        'origen': resultado['origen'],
                        'destino': resultado['destino'],
                        'impedancia_total': resultado['impedancia_total'],
                        'distancia_total': resultado['metricas']['distancia_total'],
                        'num_segmentos': resultado['metricas']['num_segmentos'],
                        'tipo_ruta': self._clasificar_tipo_ruta(resultado),
                        'geometria': ruta_geometrica,
                        'segmentos_detalle': resultado['metricas']['segmentos']
                    }
                    rutas_geometricas.append(ruta_mapeada)
                    
            except Exception as e:
                print(f"Error mapeando ruta {resultado['origen']}-{resultado['destino']}: {e}")
                continue
        
       
        if rutas_geometricas:
            gdf_rutas = gpd.GeoDataFrame(rutas_geometricas, geometry='geometria', crs=red_ciclovias.crs)
            self.rutas_mapeadas = gdf_rutas
            print(f"Geometrías generadas: {len(gdf_rutas)} rutas optimizadas")
            return gdf_rutas
        else:
            print("No se pudieron generar geometrías para las rutas")
            return gpd.GeoDataFrame()
    
    def _reconstruir_geometria_ruta(self, ruta_optima: List[str], 
                                  red_ciclovias: gpd.GeoDataFrame,
                                  resultado: Dict) -> Optional[LineString]:
        """
        Reconstruye la geometría LineString de una ruta optimizada
        utilizando los segmentos de la red de ciclovías
        """
        puntos_ruta = []
        
        for i in range(len(ruta_optima) - 1):
            nodo_inicio = ruta_optima[i]
            nodo_fin = ruta_optima[i + 1]
            
           
            segmento_geom = self._obtener_geometria_segmento(nodo_inicio, nodo_fin, red_ciclovias)
            
            if segmento_geom:
             
                if hasattr(segmento_geom, 'coords'):
                    coords = list(segmento_geom.coords)
                   
                    if i == 0:
                        puntos_ruta.extend(coords)
                    else:
                      
                        puntos_ruta.extend(coords[1:])
        
        if len(puntos_ruta) >= 2:
            return LineString(puntos_ruta)
        else:
            return None
    
    def _obtener_geometria_segmento(self, nodo_inicio: str, nodo_fin: str, 
                                  red_ciclovias: gpd.GeoDataFrame) -> Optional[LineString]:
        """
        Obtiene la geometría de un segmento entre dos nodos
        """
        try:
         
            mascara = (
                (red_ciclovias['A'] == nodo_inicio) & 
                (red_ciclovias['B'] == nodo_fin)
            ) | (
                (red_ciclovias['A'] == nodo_fin) & 
                (red_ciclovias['B'] == nodo_inicio)
            )
            
            segmentos = red_ciclovias[mascara]
            
            if not segmentos.empty:
                return segmentos.iloc[0].geometry
            else:
                return None
                
        except Exception as e:
            print(f"Error obteniendo geometría segmento {nodo_inicio}-{nodo_fin}: {e}")
            return None
    
    def _clasificar_tipo_ruta(self, resultado: Dict) -> str:
        """
        Clasifica el tipo de ruta según los atributos de infraestructura
        Basado en la metodología de evaluación del artículo
        """
        segmentos = resultado['metricas']['segmentos']
        
        if not segmentos:
            return "INDEFINIDA"
        
      
        contador_infraestructura = {}
        for segmento in segmentos:
            tipo = segmento.get('tipo_infraestructura', 3)
            contador_infraestructura[tipo] = contador_infraestructura.get(tipo, 0) + 1
        
        total_segmentos = len(segmentos)
  
        pct_segregadas = contador_infraestructura.get(1, 0) / total_segmentos * 100
        
        if pct_segregadas >= 80:
            return "CICLOVIA_SEGREGADA"
        elif pct_segregadas >= 50:
            return "MIXTA_CON_SEGREGADAS"
        elif contador_infraestructura.get(2, 0) / total_segmentos * 100 >= 50:
            return "CARRILES_BICICLETA"
        else:
            return "CALLES_COMPARTIDAS"
    
    def exportar_rutas_geoespecial(self, gdf_rutas: gpd.GeoDataFrame, 
                                 formato_salida: str = "GPKG",
                                 ruta_archivo: str = None) -> None:
        """
        Exporta las rutas optimizadas en formatos geoespaciales
        Implementa la capacidad de exportación para análisis espacial
        """
        if gdf_rutas.empty:
            print("No hay rutas para exportar")
            return
        
        if not ruta_archivo:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            ruta_archivo = f"rutas_ciclovias_optimizadas_{timestamp}"
        
        try:
            if formato_salida == "GPKG":
                archivo_completo = f"{ruta_archivo}.gpkg"
                gdf_rutas.to_file(archivo_completo, layer='rutas_optimizadas')
                print(f"Rutas exportadas a: {archivo_completo}")
                
            elif formato_salida == "Shapefile":
                archivo_completo = f"{ruta_archivo}.shp"
                gdf_rutas.to_file(archivo_completo)
                print(f"Rutas exportadas a: {archivo_completo}")
                
            elif formato_salida == "GeoJSON":
                archivo_completo = f"{ruta_archivo}.geojson"
                gdf_rutas.to_file(archivo_completo, driver='GeoJSON')
                print(f"Rutas exportadas a: {archivo_completo}")
                
            else:
                print(f"Formato no soportado: {formato_salida}")
                
        except Exception as e:
            print(f"Error exportando rutas: {e}")
    
    def generar_mapa_corredores_prioritarios(self, gdf_rutas: gpd.GeoDataFrame,
                                           metricas_centralidad: Dict) -> gpd.GeoDataFrame:
        """
        Genera mapa de corredores prioritarios para inversión
        Basado en el análisis de centralidad de intermediación del artículo
        """
        print("=== GENERANDO MAPA DE CORREDORES PRIORITARIOS ===")
  
        segmentos_utilizados = {}
        
        for _, ruta in gdf_rutas.iterrows():
            for segmento in ruta['segmentos_detalle']:
                arco = segmento['arco']
                if arco in segmentos_utilizados:
                    segmentos_utilizados[arco]['frecuencia'] += 1
                    segmentos_utilizados[arco]['impedancia_acumulada'] += segmento['impedancia']
                else:
                    segmentos_utilizados[arco] = {
                        'frecuencia': 1,
                        'impedancia_acumulada': segmento['impedancia'],
                        'tipo_infraestructura': segmento['tipo_infraestructura']
                    }

        corredores_data = []
        for arco, datos in segmentos_utilizados.items():
    
            prioridad = datos['frecuencia'] * datos['impedancia_acumulada']
  
            geometria = self._obtener_geometria_segmento(arco[0], arco[1], gdf_rutas)
            
            if geometria:
                corredores_data.append({
                    'nodo_inicio': arco[0],
                    'nodo_fin': arco[1],
                    'frecuencia_uso': datos['frecuencia'],
                    'impedancia_promedio': datos['impedancia_acumulada'] / datos['frecuencia'],
                    'prioridad_inversion': prioridad,
                    'tipo_infraestructura': datos['tipo_infraestructura'],
                    'geometria': geometria
                })
        
        if corredores_data:
            gdf_corredores = gpd.GeoDataFrame(corredores_data, geometry='geometria', crs=gdf_rutas.crs)
            
      
            gdf_corredores = gdf_corredores.sort_values('prioridad_inversion', ascending=False)
            
            print(f"Corredores identificados: {len(gdf_corredores)}")
            return gdf_corredores
        else:
            return gpd.GeoDataFrame()
    
    def crear_visualizacion_impacto(self, gdf_rutas: gpd.GeoDataFrame,
                                  escenario_base: gpd.GeoDataFrame = None) -> Dict:
        """
        Crea visualizaciones de impacto según las métricas del artículo
        Reducción de impedancia y porcentaje de desvío
        """
        metricas_visualizacion = {}
        
        if gdf_rutas.empty:
            return metricas_visualizacion
     
        impedancia_promedio = gdf_rutas['impedancia_total'].mean()
        distancia_promedio = gdf_rutas['distancia_total'].mean()
        
        metricas_visualizacion['impedancia_promedio'] = impedancia_promedio
        metricas_visualizacion['distancia_promedio'] = distancia_promedio
        metricas_visualizacion['total_rutas'] = len(gdf_rutas)

        distribucion_tipos = gdf_rutas['tipo_ruta'].value_counts().to_dict()
        metricas_visualizacion['distribucion_tipos_ruta'] = distribucion_tipos
  
        if escenario_base is not None and not escenario_base.empty:
            impedancia_base = escenario_base['impedancia_total'].mean()
            distancia_base = escenario_base['distancia_total'].mean()
            
            reduccion_impedancia = ((impedancia_base - impedancia_promedio) / impedancia_base) * 100
            porcentaje_desvio = ((distancia_promedio - distancia_base) / distancia_base) * 100
            
            metricas_visualizacion['reduccion_impedancia'] = reduccion_impedancia
            metricas_visualizacion['porcentaje_desvio'] = porcentaje_desvio
        
        print("=== MÉTRICAS DE VISUALIZACIÓN ===")
        print(f"Impedancia promedio: {impedancia_promedio:.2f} min")
        print(f"Distancia promedio: {distancia_promedio:.2f} m")
        print(f"Distribución tipos: {distribucion_tipos}")
        
        return metricas_visualizacion

def ejecutar_mapeo_rutas_articulo(resultados_optimizacion: List[Dict], 
                                red_ciclovias: gpd.GeoDataFrame,
                                config_mapeo: Dict = None) -> Tuple[gpd.GeoDataFrame, Dict]:
    """
    Función principal para ejecutar el mapeo de rutas según la metodología del artículo
    """
    print("INICIANDO MAPEO DE RUTAS OPTIMIZADAS - ARTÍCULO SPCCM")
    
    if config_mapeo is None:
        config_mapeo = {
            'formato_exportacion': 'GPKG',
            'generar_corredores': True,
            'calcular_metricas': True
        }
    
 
    mapeador = MapeadorRutasCiclovias(config_mapeo)
    

    gdf_rutas_optimizadas = mapeador.generar_geometrias_rutas_optimizadas(
        resultados_optimizacion, 
        red_ciclovias
    )
    
    if gdf_rutas_optimizadas.empty:
        print("No se generaron geometrías válidas")
        return gpd.GeoDataFrame(), {}
  
    mapeador.exportar_rutas_geoespecial(gdf_rutas_optimizadas, 
                                      config_mapeo.get('formato_exportacion', 'GPKG'))
    

    gdf_corredores = gpd.GeoDataFrame()
    if config_mapeo.get('generar_corredores', True):
        gdf_corredores = mapeador.generar_mapa_corredores_prioritarios(
            gdf_rutas_optimizadas, 
            {}
        )
        
        if not gdf_corredores.empty:
            mapeador.exportar_rutas_geoespecial(
                gdf_corredores, 
                config_mapeo.get('formato_exportacion', 'GPKG'),
                "corredores_prioritarios_ciclovias"
            )
    
    metricas_visualizacion = {}
    if config_mapeo.get('calcular_metricas', True):
        metricas_visualizacion = mapeador.crear_visualizacion_impacto(gdf_rutas_optimizadas)
    
    print("=== MAPEO COMPLETADO ===")
    print(f"Rutas mapeadas: {len(gdf_rutas_optimizadas)}")
    print(f"Corredores identificados: {len(gdf_corredores)}")
    
    return gdf_rutas_optimizadas, metricas_visualizacion

if __name__ == "__main__":
 
    print("Sistema de Mapeo SPCCM - Optimización de Rutas de Ciclovías")
