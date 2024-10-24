from distutils.core import setup, Extension

with open('Cargo.toml', 'r') as f:
    cargo_info = f.read()

def get_cargo_config(cargo_output):
    # Parse Cargo output to extract necessary information
    ...

    config = {'name': name,
              'version': version}

    return config

config = get_cargo_config(cargo_info)
extension_module = Extension('pmu_client',
                              ['src/pmu_client.rs'],
                              include_dirs=['src'])

setup(name=config['name'],
      version=config['version'],
      ext_modules=[extension_module],
      py_modules=['pmu_data_analytics'])
