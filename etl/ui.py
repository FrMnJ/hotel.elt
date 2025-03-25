import logging
from textual import on
from textual.app import App, ComposeResult
from textual.widgets import Header, Button, Input, Select, Log, Label
from textual.logging import TextualHandler
from textual.containers import Container
from output_save_options import OPTIONS
from etl import ETL
from etl_factory_impl import ETLFactoryImpl

logging.basicConfig(
    level="NOTSET",
    handlers=[TextualHandler()],
)

class UIETL(App):
    CSS_PATH = "./styles/main.tcss"
    TITLE = "ETL Hotel"

    def compose(self) -> ComposeResult:
        yield Header(id="header")
        yield Container(
            Label("Ruta del dataset a procesar:", id="dataset-input-label"),
            Input(placeholder="Escribe aquí la ruta del dataset", id="dataset-input"),
            id="dataset-container",
        )
        yield Container(
            Label("Selecciona el formato de salida:", id="output-format-label"),
            Select((option for option in OPTIONS), id="output-format"),
            id="format-container",
        )
        yield Container(
            Label("Ruta completa del archivo de salida:", id="output-path-label"),
            Input(placeholder="Escribe aquí la ruta del archivo de salida", id="output-path"),
            id="output-container",
        )
        yield Container(
            Button("Ejecutar ETL", id="run", variant="primary"),
            id="button-container",
        )
        yield Container(Log(auto_scroll=True, id="log"), id="log-container")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "run":
            dataset_input = self.query_one("#dataset-input", Input).value
            output_format = self.query_one("#output-format", Select).value
            output_path = self.query_one("#output-path", Input).value

            if not dataset_input:
                self.query_one(Log).write_line("Por favor, escribe la ruta del dataset a procesar.")
                return
            if not output_format:
                self.query_one(Log).write_line("Por favor, selecciona un formato de salida.")
                return
            if not output_path:
                self.query_one(Log).write_line("Por favor, escribe la ruta de salida para guardar los datos.")
                return

            self.query_one(Log).write_line(f"Iniciando proceso ETL para el dataset en: {dataset_input}")
            try:
                # Crear la fábrica ETL
                etl_factory = ETLFactoryImpl(
                    logger=self.query_one(Log),
                    dataset_path=dataset_input,
                    output_format=output_format,
                    output_path=output_path
                )

                # Ejecutar el flujo ETL
                etl = ETL(self.query_one(Log), etl_factory)
                etl.execute()

            except Exception as e:
                self.query_one(Log).write_line(f"Error durante el proceso ETL: {e}")
