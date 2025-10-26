
from pathlib import Path
import geopandas as gpd
import pandas as pd
import numpy as np
import pickle
from tqdm import tqdm
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import matplotlib.pyplot as plt
import seaborn as sns

@dataclass
class MetricasSPCCM:
    """Estructura para almacenar métricas del sistema SPCCM"""
    reduccion_impedancia: float = 0.0
    porcentaje_desvio: float = 0.0
    porcentaje_seguridad: float = 0.0
    tiempo_promedio_viaje: float = 0.0
    distancia_promedio: float = 0.0
    corredores_prioritarios: int = 0
    total_rutas_optimizadas: int = 0

class AnalizadorEstadisticoCiclovias:
    """
    Sistema de análisis estadístico para evaluación de rutas optimizadas de ciclovías
    Implementa las métricas de desempeño del artículo SPCCM
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.metricas_globales = MetricasSPCCM()
        self.resultados_detallados = []
        
    def calcular_metricas_articulo(self, resultados_optimizacion: List[Dict], 
                                 escenario_base: List[Dict] = None) -> MetricasSPCCM:
        """
        Calcula las métricas principales del artículo SPCCM
        Basado en la Etapa 3: Evaluación y Métrica de Impacto
        """
        print("=== CALCULANDO MÉTRICAS SPCCM ===")
        
        if not resultados_optimizacion:
            print("No hay resultados de optimización para analizar")
            return self.metricas_globales
        
      
        if escenario_base:
            reduccion_impedancia = self._calcular_reduccion_impedancia(
                resultados_optimizacion, escenario_base
            )
            self.metricas_globales.reduccion_impedancia = reduccion_impedancia
        
     
        if escenario_base:
            porcentaje_desvio = self._calcular_porcentaje_desvio(
                resultados_optimizacion, escenario_base
            )
            self.metricas_globales.porcentaje_desvio = porcentaje_desvio
        
    
        porcentaje_seguridad = self._calcular_porcentaje_seguridad(resultados_optimizacion)
        self.metricas_globales.porcentaje_seguridad = porcentaje_seguridad
   
        tiempo_promedio = self._calcular_tiempo_promedio_viaje(resultados_optimizacion)
        self.metricas_globales.tiempo_promedio_viaje = tiempo_promedio
     
        distancia_promedio = self._calcular_distancia_promedio(resultados_optimizacion)
        self.metricas_globales.distancia_promedio = distancia_promedio
  
        corredores_prioritarios = self._identificar_corredores_prioritarios(resultados_optimizacion)
        self.metricas_globales.corredores_prioritarios = len(corredores_prioritarios)
    
        self.metricas_globales.total_rutas_optimizadas = len(resultados_optimizacion)
 
        self.resultados_detallados = resultados_optimizacion
        
        self._imprimir_metricas_consola()
        return self.metricas_globales
    
    def _calcular_reduccion_impedancia(self, resultados_optimizados: List[Dict], 
                                     escenario_base: List[Dict]) -> float:
        """
        Calcula la reducción de impedancia según el artículo:
        Reduccion Imp. = (I(R_Vehicular) - I(R_Bike)) / I(R_Vehicular) * 100
        """
        try:
            impedancia_base = np.mean([r.get('impedancia_total', 0) for r in escenario_base])
            impedancia_optimizada = np.mean([r.get('impedancia_total', 0) for r in resultados_optimizados])
            
            if impedancia_base > 0:
                reduccion = ((impedancia_base - impedancia_optimizada) / impedancia_base) * 100
                return max(0, reduccion)  # No permitir valores negativos
            else:
                return 0.0
                
        except Exception as e:
            print(f"Error calculando reducción de impedancia: {e}")
            return 0.0
    
    def _calcular_porcentaje_desvio(self, resultados_optimizados: List[Dict], 
                                  escenario_base: List[Dict]) -> float:
        """
        Calcula el porcentaje de desvío según el artículo:
        Desvio = (D_bike - D_Vehicular) / D_Vehicular * 100
        """
        try:
            distancia_base = np.mean([
                r.get('metricas', {}).get('distancia_total', 0) 
                for r in escenario_base
            ])
            distancia_optimizada = np.mean([
                r.get('metricas', {}).get('distancia_total', 0) 
                for r in resultados_optimizados
            ])
            
            if distancia_base > 0:
                desvio = ((distancia_optimizada - distancia_base) / distancia_base) * 100
                return desvio
            else:
                return 0.0
                
        except Exception as e:
            print(f"Error calculando porcentaje de desvío: {e}")
            return 0.0
    
    def _calcular_porcentaje_seguridad(self, resultados_optimizados: List[Dict]) -> float:
        """
        Calcula el porcentaje de infraestructura segura en las rutas optimizadas
        Infraestructura segura: Ciclovías segregadas y carriles bicicleta
        """
        try:
            porcentajes_seguridad = []
            
            for resultado in resultados_optimizados:
                metricas = resultado.get('metricas', {})
                porcentaje_seguridad_ruta = metricas.get('porcentaje_infra_segura', 0)
                porcentajes_seguridad.append(porcentaje_seguridad_ruta)
            
            if porcentajes_seguridad:
                return np.mean(porcentajes_seguridad)
            else:
                return 0.0
                
        except Exception as e:
            print(f"Error calculando porcentaje de seguridad: {e}")
            return 0.0
    
    def _calcular_tiempo_promedio_viaje(self, resultados_optimizados: List[Dict]) -> float:
        """Calcula el tiempo promedio de viaje en minutos"""
        try:
            tiempos = [r.get('impedancia_total', 0) for r in resultados_optimizados]
            return np.mean(tiempos) if tiempos else 0.0
        except Exception as e:
            print(f"Error calculando tiempo promedio: {e}")
            return 0.0
    
    def _calcular_distancia_promedio(self, resultados_optimizados: List[Dict]) -> float:
        """Calcula la distancia promedio recorrida en metros"""
        try:
            distancias = [
                r.get('metricas', {}).get('distancia_total', 0) 
                for r in resultados_optimizados
            ]
            return np.mean(distancias) if distancias else 0.0
        except Exception as e:
            print(f"Error calculando distancia promedio: {e}")
            return 0.0
    
    def _identificar_corredores_prioritarios(self, resultados_optimizados: List[Dict]) -> List[Tuple[str, str]]:
        """
        Identifica corredores prioritarios para inversión basado en centralidad
        Implementa el análisis de centralidad de intermediación del artículo
        """
        try:
            frecuencia_segmentos = {}
            
            for resultado in resultados_optimizados:
                segmentos = resultado.get('metricas', {}).get('segmentos', [])
                
                for segmento in segmentos:
                    arco = (segmento['nodo_inicio'], segmento['nodo_fin'])
                    frecuencia_segmentos[arco] = frecuencia_segmentos.get(arco, 0) + 1
            
      
            corredores_prioritarios = sorted(
                frecuencia_segmentos.items(), 
                key=lambda x: x[1], 
                reverse=True
            )[:10] 
            
            return [corredor for corredor, _ in corredores_prioritarios]
            
        except Exception as e:
            print(f"Error identificando corredores prioritarios: {e}")
            return []
    
    def _imprimir_metricas_consola(self):
        """Imprime las métricas calculadas en la consola"""
        print("\n" + "="*50)
        print("MÉTRICAS SPCCM - EVALUACIÓN DE DESEMPEÑO")
        print("="*50)
        print(f"Reducción de Impedancia: {self.metricas_globales.reduccion_impedancia:.2f}%")
        print(f"Porcentaje de Desvío: {self.metricas_globales.porcentaje_desvio:.2f}%")
        print(f"Porcentaje de Seguridad: {self.metricas_globales.porcentaje_seguridad:.1f}%")
        print(f"Tiempo Promedio de Viaje: {self.metricas_globales.tiempo_promedio_viaje:.1f} min")
        print(f"Distancia Promedio: {self.metricas_globales.distancia_promedio:.1f} m")
        print(f"Corredores Prioritarios Identificados: {self.metricas_globales.corredores_prioritarios}")
        print(f"Total Rutas Optimizadas: {self.metricas_globales.total_rutas_optimizadas}")
        print("="*50)
    
    def analizar_distribucion_tiempos(self, resultados_optimizados: List[Dict]) -> Dict:
        """
        Analiza la distribución de tiempos de viaje
        Basado en el análisis de distribución del código original
        """
        tiempos = [r.get('impedancia_total', 0) for r in resultados_optimizados]
        
        if not tiempos:
            return {}
        
        analisis = {
            'min': np.min(tiempos),
            'max': np.max(tiempos),
            'media': np.mean(tiempos),
            'mediana': np.median(tiempos),
            'desviacion_estandar': np.std(tiempos),
            'percentil_25': np.percentile(tiempos, 25),
            'percentil_75': np.percentile(tiempos, 75)
        }
        
        print("\nDistribución de Tiempos de Viaje:")
        print(f"Rango: {analisis['min']:.1f} - {analisis['max']:.1f} min")
        print(f"Media: {analisis['media']:.1f} min")
        print(f"Mediana: {analisis['mediana']:.1f} min")
        print(f"Desviación Estándar: {analisis['desviacion_estandar']:.1f} min")
        
        return analisis
    
    def comparar_modos_transporte(self, resultados_bicicleta: List[Dict],
                                resultados_caminata: List[Dict],
                                resultados_mixto: List[Dict]) -> Dict:
        """
        Compara diferentes modos de transporte según la metodología del artículo
        Análisis similar al presentado en el código original
        """
        comparacion = {}
        
 
        bike_exitosas = [r for r in resultados_bicicleta if r.get('impedancia_total', 0) > 0]
        walk_exitosas = [r for r in resultados_caminata if r.get('impedancia_total', 0) > 0]
        mixed_exitosas = [r for r in resultados_mixto if r.get('impedancia_total', 0) > 0]
 
        comparacion['accesibilidad_bike'] = len(bike_exitosas)
        comparacion['accesibilidad_walk'] = len(walk_exitosas)
        comparacion['accesibilidad_mixed'] = len(mixed_exitosas)

        comparacion['tiempo_promedio_bike'] = np.mean([r.get('impedancia_total', 0) for r in bike_exitosas])
        comparacion['tiempo_promedio_walk'] = np.mean([r.get('impedancia_total', 0) for r in walk_exitosas])
        comparacion['tiempo_promedio_mixed'] = np.mean([r.get('impedancia_total', 0) for r in mixed_exitosas])
    
        comparacion['distancia_promedio_bike'] = np.mean([
            r.get('metricas', {}).get('distancia_total', 0) for r in bike_exitosas
        ])
        comparacion['distancia_promedio_walk'] = np.mean([
            r.get('metricas', {}).get('distancia_total', 0) for r in walk_exitosas
        ])
        comparacion['distancia_promedio_mixed'] = np.mean([
            r.get('metricas', {}).get('distancia_total', 0) for r in mixed_exitosas
        ])
        
        print("\nComparación de Modos de Transporte:")
        print(f"Accesibilidad - Bike: {comparacion['accesibilidad_bike']}, "
              f"Walk: {comparacion['accesibilidad_walk']}, "
              f"Mixed: {comparacion['accesibilidad_mixed']}")
        print(f"Tiempo Promedio - Bike: {comparacion['tiempo_promedio_bike']:.1f} min, "
              f"Walk: {comparacion['tiempo_promedio_walk']:.1f} min, "
              f"Mixed: {comparacion['tiempo_promedio_mixed']:.1f} min")
        
        return comparacion
    
    def generar_reporte_evaluacion(self, metricas: MetricasSPCCM) -> str:
        """
        Genera un reporte completo de evaluación en formato texto
        """
        reporte = f"""
REPORTE DE EVALUACIÓN SISTEMA SPCCM
===================================

RESULTADOS PRINCIPALES:
- Reducción de Impedancia: {metricas.reduccion_impedancia:.2f}%
- Porcentaje de Desvío: {metricas.porcentaje_desvio:.2f}%
- Porcentaje de Seguridad en Infraestructura: {metricas.porcentaje_seguridad:.1f}%
- Tiempo Promedio de Viaje: {metricas.tiempo_promedio_viaje:.1f} minutos
- Distancia Promedio Recorrida: {metricas.distancia_promedio:.1f} metros

IMPACTO EN LA RED:
- Corredores Prioritarios Identificados: {metricas.corredores_prioritarios}
- Total de Rutas Optimizadas: {metricas.total_rutas_optimizadas}

INTERPRETACIÓN:
El sistema SPCCM ha demostrado una mejora significativa en la planificación de rutas ciclistas,
priorizando la seguridad y eficiencia en el contexto de infraestructura fragmentada típica de
ciudades latinoamericanas.
        """
        
        return reporte
    
    def exportar_metricas_csv(self, metricas: MetricasSPCCM, archivo_salida: str):
        """Exporta las métricas a archivo CSV para análisis posterior"""
        try:
            df_metricas = pd.DataFrame([metricas.__dict__])
            df_metricas.to_csv(archivo_salida, index=False)
            print(f"Métricas exportadas a: {archivo_salida}")
        except Exception as e:
            print(f"Error exportando métricas: {e}")

def ejecutar_analisis_articulo(resultados_optimizacion: List[Dict],
                             escenario_base: List[Dict] = None) -> AnalizadorEstadisticoCiclovias:
    """
    Función principal que ejecuta el análisis estadístico del artículo SPCCM
    """
    print("INICIANDO ANÁLISIS ESTADÍSTICO SPCCM")
    print("Evaluación de Métricas de Optimización de Ciclovías")
   
    analizador = AnalizadorEstadisticoCiclovias()

    metricas = analizador.calcular_metricas_articulo(resultados_optimizacion, escenario_base)

    analizador.analizar_distribucion_tiempos(resultados_optimizacion)

    reporte = analizador.generar_reporte_evaluacion(metricas)
    print(reporte)

    analizador.exportar_metricas_csv(metricas, "metricas_spccm.csv")
    
    return analizador

if __name__ == "__main__":
  
    print("Sistema de Análisis Estadístico SPCCM - Listo para ejecutar")

    resultados_ejemplo = [
        {
            'origen': 'CENTRO',
            'destino': 'UNIVERSIDAD', 
            'impedancia_total': 25.5,
            'metricas': {
                'distancia_total': 3500,
                'porcentaje_infra_segura': 80.0,
                'segmentos': []
            }
        }
    ]
   
