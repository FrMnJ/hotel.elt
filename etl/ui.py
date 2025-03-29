import logging
from textual import on
from textual.app import App, ComposeResult
from textual.widgets import Header, Button, Input, Select, Log, Label
from textual.logging import TextualHandler
from textual.containers import Container
from etl import ETL
from etl_factory_impl import ETLFactoryImpl
from output_save_options import OPTIONS

logging.basicConfig(
    level="NOTSET",
    handlers=[TextualHandler()],
)

class UIETL(App):
    CSS_PATH = "./styles/main.tcss"
    TITLE = "ETL Hotel"
    output_format = Select.BLANK
    is_sql = False
    def compose(self) -> ComposeResult:
        yield Header(id="header")
        yield Container(
            Label("Selecciona el formato de salida:"),
            Select(option for option in OPTIONS),
            Container(
                Label("Ruta completa del archivo de salida:", id="name-output-label"),
                Input(id="name-output"),
                id="name-output-container",
            ),
            Container(
                Label("Ingresa el nombre de la tabla:"),
                Input(id="table-name-input"),
                id="table-name-container"
            ),
            id="form-container",
        )
        yield Container(Button("Ejecutar ETL", id="run", variant="primary"), id="button-container")
        yield Container(Log(auto_scroll=True, id="log"), id="log-container")
    
    def update_label(self) -> None:
        label = self.query_one("#name-output-label", Label)
        label.update("URI de conexión de bases de datos:" if self.is_sql else "Ruta completa del archivo de salida:")
    
    @on(Select.Changed)
    def select_changed(self, event: Select.Changed) -> None:
        self.output_format = event.value
        self.is_sql = self.output_format == "sql"
        self.update_label()
        if not self.is_sql:
            self.query_one("#table-name-container").add_class("hidden")
        else: 
            self.query_one("#table-name-container").remove_class("hidden") 
            
        if self.output_format is not Select.BLANK:
            self.query_one(Log).write_line(f"Opción seleccionada: {self.output_format}")
    
    def get_output_destination(self) -> str:   
        return self.query_one("#name-output", Input).value

    def get_table_name(self):
        if not self.is_sql:
            return None
        return self.query_one("#table-name-input", Input).value

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if self.output_format is Select.BLANK:
            self.query_one(Log).write_line("Selecciona un formato de salida")
        elif event.button.id == "run" and not self.executing:
            self.executing = True
            my_logger = self.query_one(Log)
            my_factory = ETLFactoryImpl(
                my_logger,
                self.output_format,
                self.get_output_destination(),
                self.get_table_name(),
            )
            my_etl = ETL(my_logger, my_factory)
            self.query_one(Log).write_line("Ejecutando ETL...")
            my_etl.execute()
            self.executing = False

    def on_mount(self) -> None:
        self.executing = False
        self.query_one("#table-name-container").add_class("hidden")
        self.query_one(Log).write_line("Inicializando la aplicacion...")