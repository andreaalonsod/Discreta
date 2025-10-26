
import pandas as pd
import geopandas as gpd
import networkx as nx
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from enum import Enum
import heapq
from pathlib import Path


from helper_functions import create_bike_graph, ckdnearest, process_results, load_files

class TipoInfraestructura(Enum):
    CICLOVIA_SEGREGADA = 1
    CARRIL_BICICLETA = 2
    CALLE_COMPARTIDA = 3
    SIN_INFRAESTRUCTURA = 4

@dataclass
class ConfiguracionOptimizacion:
    """Configuración del sistema de optimización"""
    coeficiente_seguridad: float = 0.3
    coeficiente_pendiente: float = 0.05
    coeficiente_trafico: float = 0.0001
    coeficiente_velocidad: float = 0.01
    velocidad_promedio_bicicleta: float = 15.0  
    umbral_caminata: float = 500.0  
    velocidad_caminata: float = 5.0  

class SistemaOptimizacionCiclovias:
    """
    Sistema principal para optimización de rutas ciclistas
    Implementa la metodología descrita en el artículo
    """
    
    def __init__(self, config: ConfiguracionOptimizacion):
        self.config = config
        self.grafo_ciclista = None
        self.nodos_snapped = {}
        self.resultados_optimizacion = []
        
    def cargar_red_ciclista(self, archivo_enlaces: str) -> nx.DiGraph:
        """
        Carga y prepara la red ciclista desde un archivo de enlaces
        Basado en la estructura de la metodología: G = (N, A)
        """
        try:
           
            enlaces = gpd.read_file(archivo_enlaces)
            
            
            enlaces['impedancia_ciclista'] = enlaces.apply(
                self._calcular_impedancia_multicriterio, axis=1
            )
            
           
            self.grafo_ciclista = create_bike_graph(enlaces, 'impedancia_ciclista')
            
            print(f"Red ciclista cargada: {len(self.grafo_ciclista.nodes)} nodos, {len(self.grafo_ciclista.edges)} enlaces")
            return self.grafo_ciclista
            
        except Exception as e:
            print(f"Error cargando red ciclista: {e}")
            return None
    
    def _calcular_impedancia_multicriterio(self, enlace: pd.Series) -> float:
        """
        Calcula la impedancia multicriterio según la metodología del artículo
        C_i,time+attributes = (l_i / v) * (1 + Σ(β_k * x_k,i))
        """
        
        distancia_km = enlace['longitud'] / 1000  
        tiempo_base = (distancia_km / self.config.velocidad_promedio_bicicleta) * 60
        
       
        factor_seguridad = self._obtener_factor_seguridad(enlace.get('tipo_infraestructura', 3))
        factor_pendiente = self.config.coeficiente_pendiente * abs(enlace.get('pendiente', 0))
        factor_trafico = self.config.coeficiente_trafico * enlace.get('volumen_vehicular', 0)
        factor_velocidad = self.config.coeficiente_velocidad * max(0, enlace.get('velocidad_vehicular', 0) - 30)
        
        factores_totales = factor_seguridad + factor_pendiente + factor_trafico + factor_velocidad
        
        impedancia = tiempo_base * (1 + factores_totales)
        return impedancia
    
    def _obtener_factor_seguridad(self, tipo_infra: int) -> float:
        """Convierte tipo de infraestructura en factor de seguridad"""
        factores = {
            1: 0.1,  
            2: 0.3,  
            3: 0.6,  
            4: 0.9   
        }
        return factores.get(tipo_infra, 0.6)
    
    def snap_puntos_red(self, puntos: gpd.GeoDataFrame, red: gpd.GeoDataFrame) -> Dict:
        """
        Asocia puntos (TAZs, paradas) a los nodos más cercanos de la red
        Usa la función ckdnearest existente
        """
        puntos_snapped = ckdnearest(puntos, red)
        self.nodos_snapped = dict(zip(puntos_snapped['id'], puntos_snapped['node_id']))
        return self.nodos_snapped
    
    def ejecutar_optimizacion_rutas(self, pares_od: List[Tuple[str, str]]) -> List[Dict]:
        """
        Ejecuta la optimización de rutas para todos los pares O-D
        Implementa el algoritmo de Dijkstra según la metodología
        """
        resultados = []
        
        for origen, destino in pares_od:
            try:
               
                nodo_origen = self.nodos_snapped.get(origen)
                nodo_destino = self.nodos_snapped.get(destino)
                
                if not nodo_origen or not nodo_destino:
                    print(f"Advertencia: No se encontraron nodos para O-D ({origen}, {destino})")
                    continue
                
                
                ruta_optima, impedancia_total = self._dijkstra_ciclista(
                    nodo_origen, nodo_destino
                )
                
               
                metricas = self._calcular_metricas_ruta(ruta_optima)
                
                resultado = {
                    'origen': origen,
                    'destino': destino,
                    'ruta_optima': ruta_optima,
                    'impedancia_total': impedancia_total,
                    'metricas': metricas
                }
                
                resultados.append(resultado)
                print(f"Ruta optimizada: {origen} -> {destino} | Impedancia: {impedancia_total:.2f} min")
                
            except Exception as e:
                print(f"Error optimizando ruta {origen}-{destino}: {e}")
                continue
        
        self.resultados_optimizacion = resultados
        return resultados
    
    def _dijkstra_ciclista(self, origen: int, destino: int) -> Tuple[List[int], float]:
        """
        Implementación del algoritmo de Dijkstra para encontrar la ruta de mínima impedancia
        """
        distancias = {nodo: float('inf') for nodo in self.grafo_ciclista.nodes}
        predecesores = {nodo: None for nodo in self.grafo_ciclista.nodes}
        distancias[origen] = 0
        
        cola_prioridad = [(0, origen)]
        
        while cola_prioridad:
            distancia_actual, nodo_actual = heapq.heappop(cola_prioridad)
            
            if nodo_actual == destino:
                break
            
            if distancia_actual > distancias[nodo_actual]:
                continue
            
            for vecino, atributos in self.grafo_ciclista[nodo_actual].items():
                peso = atributos.get('weight', float('inf'))
                nueva_distancia = distancia_actual + peso
                
                if nueva_distancia < distancias[vecino]:
                    distancias[vecino] = nueva_distancia
                    predecesores[vecino] = nodo_actual
                    heapq.heappush(cola_prioridad, (nueva_distancia, vecino))
        
       
        ruta = self._reconstruir_ruta(predecesores, destino)
        return ruta, distancias.get(destino, float('inf'))
    
    def _reconstruir_ruta(self, predecesores: Dict, destino: int) -> List[int]:
        """Reconstruye la ruta desde el destino hasta el origen"""
        ruta = []
        actual = destino
        
        while actual is not None:
            ruta.append(actual)
            actual = predecesores.get(actual)
        
        return ruta[::-1]  
    
    def _calcular_metricas_ruta(self, ruta: List[int]) -> Dict:
        """Calcula métricas de desempeño para una ruta"""
        if len(ruta) < 2:
            return {}
        
        
        distancia_total = 0
        segmentos = []
        
        for i in range(len(ruta) - 1):
            nodo_a, nodo_b = ruta[i], ruta[i + 1]
            if self.grafo_ciclista.has_edge(nodo_a, nodo_b):
                edge_data = self.grafo_ciclista[nodo_a][nodo_b]
                distancia_total += edge_data.get('length', 0)
                segmentos.append({
                    'nodo_inicio': nodo_a,
                    'nodo_fin': nodo_b,
                    'impedancia': edge_data.get('weight', 0)
                })
        
        return {
            'distancia_total': distancia_total,
            'num_segmentos': len(segmentos),
            'segmentos': segmentos
        }
    
    def comparar_escenarios(self, escenario_base: List[Dict]) -> Dict:
        """
        Compara diferentes escenarios según la metodología del artículo
        Escenario 1: No optimizado (ruta vehicular)
        Escenario 2: Optimizado por impedancia ciclista
        Escenario 3: Con nueva infraestructura
        """
        metricas_comparativas = {}
        
       
        impedancia_base = np.mean([r['impedancia_total'] for r in escenario_base])
        impedancia_optimizada = np.mean([r['impedancia_total'] for r in self.resultados_optimizacion])
        
        reduccion_impedancia = ((impedancia_base - impedancia_optimizada) / impedancia_base) * 100
        
       
        distancia_base = np.mean([r['metricas']['distancia_total'] for r in escenario_base])
        distancia_optimizada = np.mean([r['metricas']['distancia_total'] for r in self.resultados_optimizacion])
        
        porcentaje_desvio = ((distancia_optimizada - distancia_base) / distancia_base) * 100
        
        metricas_comparativas = {
            'reduccion_impedancia': reduccion_impedancia,
            'porcentaje_desvio': porcentaje_desvio,
            'impedancia_promedio_base': impedancia_base,
            'impedancia_promedio_optimizada': impedancia_optimizada,
            'distancia_promedio_base': distancia_base,
            'distancia_promedio_optimizada': distancia_optimizada
        }
        
        return metricas_comparativas
    
    def generar_reporte_evaluacion(self, metricas: Dict) -> str:
        """Genera reporte de evaluación del sistema"""
        reporte = f"""
=== EVALUACIÓN SISTEMA OPTIMIZACIÓN CICLOVÍAS ===

Métricas de Desempeño:
- Reducción de Impedancia: {metricas.get('reduccion_impedancia', 0):.2f}%
- Porcentaje de Desvío: {metricas.get('porcentaje_desvio', 0):.2f}%
- Impedancia Promedio (Base): {metricas.get('impedancia_promedio_base', 0):.2f} min
- Impedancia Promedio (Optimizada): {metricas.get('impedancia_promedio_optimizada', 0):.2f} min
- Distancia Promedio (Base): {metricas.get('distancia_promedio_base', 0):.2f} m
- Distancia Promedio (Optimizada): {metricas.get('distancia_promedio_optimizada', 0):.2f} m

Rutas Optimizadas: {len(self.resultados_optimizacion)}
        """
        return reporte


def ejecutar_sistema_optimizacion(config_path: str = None):
    """
    Función principal que ejecuta todo el sistema de optimización
    """
    
    config = ConfiguracionOptimizacion()
    
   
    sistema = SistemaOptimizacionCiclovias(config)
    
    
    archivo_enlaces = "data/red_ciclista.gpkg"  
    grafo = sistema.cargar_red_ciclista(archivo_enlaces)
    
    if grafo is None:
        print("Error: No se pudo cargar la red ciclista")
        return
    
   
    pares_od = [
        ("TAZ_001", "TAZ_100"),
        ("TAZ_050", "TAZ_150"),
        ("TAZ_025", "TAZ_075")
    ]
    
   
    resultados = sistema.ejecutar_optimizacion_rutas(pares_od)
    
   
    print("Optimización completada exitosamente")
    return sistema

if __name__ == "__main__":
    
    sistema = ejecutar_sistema_optimizacion()
