#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <sys/time.h>
#include <getopt.h>

/* Добавляем заголовочный файл нашего кэша */
#include "../lib/vtpc.h"

#define BASE_10 10
#define ALIGNMENT 512
#define DEFAULT_CACHE_BLOCKS 1000
#define DEFAULT_BLOCK_SIZE 4096

/* Структура для параметров кэширования */
typedef struct {
    char* type;          /* "std", "direct", "vtpc" */
    int cache_blocks;    /* количество блоков в кэше */
    int block_size;      /* размер блока кэша */
    int passes;          /* количество проходов */
} cache_config_t;

/* Структура для метрик производительности */
typedef struct {
    struct timeval start_time;
    struct timeval end_time;
    size_t bytes_processed;
    size_t operations_count;
} performance_metrics_t;

void init_metrics(performance_metrics_t* metrics) {
    metrics->bytes_processed = 0;
    metrics->operations_count = 0;
}

void start_timer(performance_metrics_t* metrics) {
    gettimeofday(&metrics->start_time, NULL);
}

void stop_timer(performance_metrics_t* metrics) {
    gettimeofday(&metrics->end_time, NULL);
}

double get_elapsed_time(performance_metrics_t* metrics) {
    return (metrics->end_time.tv_sec - metrics->start_time.tv_sec) + 
           (metrics->end_time.tv_usec - metrics->start_time.tv_usec) / 1000000.0;
}

double get_throughput_mbps(performance_metrics_t* metrics) {
    double elapsed = get_elapsed_time(metrics);
    if (elapsed == 0) return 0;
    return (metrics->bytes_processed / (1024.0 * 1024.0)) / elapsed;
}

double get_iops(performance_metrics_t* metrics) {
    double elapsed = get_elapsed_time(metrics);
    if (elapsed == 0) return 0;
    return metrics->operations_count / elapsed;
}

void print_metrics(performance_metrics_t* metrics, cache_config_t* cache_config) {
    double elapsed = get_elapsed_time(metrics);
    double throughput = get_throughput_mbps(metrics);
    double iops = get_iops(metrics);
    
    printf("\n=== МЕТРИКИ ПРОИЗВОДИТЕЛЬНОСТИ ===\n");
    printf("Режим кэширования: %s\n", cache_config->type);
    printf("Общее время: %.3f секунд\n", elapsed);
    printf("Обработано данных: %.2f MB\n", metrics->bytes_processed / (1024.0 * 1024.0));
    printf("Пропускная способность: %.2f MB/s\n", throughput);
    printf("IOPS: %.2f операций/сек\n", iops);
    printf("================================\n\n");
}

int parse_range(const char *range, size_t *left, size_t *right) {
    if (range == NULL) {
        return -1;
    }
    
    int temp_left;
    int temp_right;
    int parsed = sscanf(range, "%d-%d", &temp_left, &temp_right);
    
    if (parsed != 2) {
        return -1;
    }

    if (temp_left < 0 || temp_right < 0 || temp_left > temp_right) {
        return -1;
    }
    
    *left = (size_t)temp_left;
    *right = (size_t)temp_right;
    
    return 0;
}

void print_usage() {
    printf("Использование:\n");
    printf("  Старый формат (7 параметров):\n");
    printf("    loader [rw] [block_size] [block_count] [file] [range] [direct] [type]\n");
    printf("\n  Новый формат с кэшированием:\n");
    printf("    loader [rw] [block_size] [block_count] [file] [range] [direct] [type] [cache_type] [cache_blocks] [passes]\n");
    printf("\n  Параметры кэширования:\n");
    printf("    cache_type: std (стандартный), direct (без кэша), vtpc (наш кэш)\n");
    printf("    cache_blocks: количество блоков в кэше (только для vtpc, по умолчанию %d)\n", DEFAULT_CACHE_BLOCKS);
    printf("    passes: количество проходов (по умолчанию 1)\n");
    printf("\n  Примеры:\n");
    printf("    loader r 4096 100 test.dat 0-1000 off sequential std\n");
    printf("    loader r 4096 100 test.dat 0-1000 off sequential vtpc 1000\n");
    printf("    loader r 4096 100 test.dat 0-1000 off sequential vtpc 1000 2  # 2 прохода\n");
}

int main(int argc, char *argv[]) {
    /* Параметры по умолчанию */
    cache_config_t cache_config = {
        .type = "std",
        .cache_blocks = DEFAULT_CACHE_BLOCKS,
        .block_size = DEFAULT_BLOCK_SIZE,
        .passes = 1
    };
    
    /* Старые параметры (для обратной совместимости) */
    char* rw = NULL;
    size_t block_size = 0;
    size_t block_count = 0;
    char* file_path = NULL;
    char* range_str = NULL;
    bool direct = false;
    char* type = NULL;
    
    /* Разбор аргументов в зависимости от их количества */
    if (argc == 8) {
        /* Старый формат: 7 параметров + имя программы */
        rw = argv[1];
        block_size = (size_t)strtoull(argv[2], NULL, BASE_10);
        block_count = (size_t)strtoull(argv[3], NULL, BASE_10);
        file_path = argv[4];
        range_str = argv[5];
        direct = strcmp(argv[6], "on") == 0;
        type = argv[7];
        
        /* Определяем тип кэширования на основе direct */
        cache_config.type = direct ? "direct" : "std";
        
    } else if (argc >= 9) {
        /* Новый формат с кэшированием */
        rw = argv[1];
        block_size = (size_t)strtoull(argv[2], NULL, BASE_10);
        block_count = (size_t)strtoull(argv[3], NULL, BASE_10);
        file_path = argv[4];
        range_str = argv[5];
        direct = strcmp(argv[6], "on") == 0;
        type = argv[7];
        cache_config.type = argv[8];
        
        /* Дополнительные параметры для vtpc кэша */
        if (argc >= 10) {
            cache_config.cache_blocks = atoi(argv[9]);
        }
        /* Новый параметр: количество проходов */
        if (argc >= 11) {
            cache_config.passes = atoi(argv[10]);
        }
    } else {
        printf("Неверное количество аргументов: %d\n", argc);
        print_usage();
        return -1;
    }
    
    /* Проверка обязательных параметров */
    if (!rw || !file_path || !range_str || !type) {
        printf("Обязательные параметры не указаны\n");
        print_usage();
        return -1;
    }
    
    /* Парсинг диапазона */
    size_t left_range, right_range;
    if (parse_range(range_str, &left_range, &right_range) != 0) {
        printf("Неверный формат диапазона: %s. Используйте: start-end (с start <= end)\n", range_str);
        return -1;
    }
    
    /* Проверка совместимости параметров */
    if (strcmp(cache_config.type, "direct") == 0 && (block_size % ALIGNMENT != 0)) {
        printf("Для O_DIRECT block_size должен быть кратен %d\n", ALIGNMENT);
        return -1;
    }
    
    /* Инициализация нашего кэша если выбран режим vtpc */
    if (strcmp(cache_config.type, "vtpc") == 0) {
        if (vtpc_cache_init() != 0) {
            printf("Ошибка инициализации VTPC кэша\n");
            return -1;
        }
        printf("Инициализирован VTPC кэш: блоков=%d\n", cache_config.cache_blocks);
    }
    
    /* Метрики производительности */
    performance_metrics_t metrics;
    init_metrics(&metrics);
    
    printf("Запуск нагрузчика с параметрами:\n");
    printf("  Режим: %s\n", rw);
    printf("  Размер блока: %zu\n", block_size);
    printf("  Количество блоков: %zu\n", block_count);
    printf("  Файл: %s\n", file_path);
    printf("  Диапазон: %zu-%zu\n", left_range, right_range);
    printf("  Тип доступа: %s\n", type);
    printf("  Режим кэширования: %s\n", cache_config.type);
    if (cache_config.passes > 1) {
        printf("  Количество проходов: %d\n", cache_config.passes);
    }
    
    /* Переменные для работы с файлом в разных режимах */
    int std_fd = -1;
    FILE* std_file = NULL;
    int vtpc_fd = -1;
    
    /* Открытие файла в зависимости от режима кэширования */
    if (strcmp(cache_config.type, "vtpc") == 0) {
        /* Используем наш кэш */
        int flags = O_RDWR;
        if (strcmp(rw, "w") == 0) {
            flags |= O_CREAT;
        }
        vtpc_fd = vtpc_open(file_path, flags, 0644);
        if (vtpc_fd < 0) {
            printf("Ошибка открытия файла через VTPC: %s\n", strerror(errno));
            return -1;
        }
    } else if (strcmp(cache_config.type, "direct") == 0) {
        /* Прямой доступ (без кэша ОС) */
        std_fd = open(file_path, O_RDWR | O_CREAT | O_DIRECT, 0644);
        if (std_fd == -1) {
            perror("Ошибка открытия файла с O_DIRECT");
            return -1;
        }
    } else {
        /* Стандартный режим (с кэшем ОС) */
        std_file = fopen(file_path, "r+b");
        if (std_file == NULL) {
            std_file = fopen(file_path, "w+b");
            if (std_file == NULL) {
                perror("Ошибка открытия файла для чтения/записи");
                return -1;
            }
        }
    }
    
    /* Определение размера файла */
    size_t file_size = 0;
    if (strcmp(cache_config.type, "vtpc") == 0) {
        off_t current_pos = vtpc_lseek(vtpc_fd, 0, SEEK_CUR);
        file_size = vtpc_lseek(vtpc_fd, 0, SEEK_END);
        vtpc_lseek(vtpc_fd, current_pos, SEEK_SET);
    } else if (strcmp(cache_config.type, "direct") == 0) {
        file_size = lseek(std_fd, 0, SEEK_END);
        lseek(std_fd, 0, SEEK_SET);
    } else {
        fseek(std_file, 0, SEEK_END);
        file_size = ftell(std_file);
        fseek(std_file, 0, SEEK_SET);
    }
    
    /* Настройка диапазона работы */
    size_t start_pos, end_pos;
    bool unlimited_range = false;
    
    if (left_range == 0 && right_range == 0) {
        start_pos = 0;
        if (strcmp(rw, "w") == 0 && strcmp(type, "sequential") == 0) {
            unlimited_range = true;
            end_pos = 0;
        } else {
            end_pos = file_size;
        }
    } else {
        start_pos = left_range;
        end_pos = right_range;
        if (strcmp(rw, "r") == 0 && end_pos > file_size) {
            end_pos = file_size;
        }
    }
    
    if (!unlimited_range && start_pos > end_pos) {
        printf("Неверный диапазон: start не может быть больше end\n");
        goto cleanup;
    }
    
    size_t range_size = end_pos - start_pos;
    
    if (!unlimited_range && range_size < block_size && block_count > 0) {
        printf("Размер диапазона (%zu) меньше размера блока (%zu)\n", range_size, block_size);
        goto cleanup;
    }
    
    /* Инициализация буфера */
    char* buffer = NULL;
    if (strcmp(cache_config.type, "direct") == 0) {
        int result = posix_memalign((void**)&buffer, ALIGNMENT, block_size);
        if (result != 0) {
            printf("Ошибка выделения памяти для O_DIRECT: %s\n", strerror(result));
            goto cleanup;
        }
    } else {
        buffer = malloc(block_size);
        if (buffer == NULL) {
            printf("Ошибка выделения памяти\n");
            goto cleanup;
        }
    }
    
    /* Инициализация генератора случайных чисел */
    if (strcmp(type, "random") == 0) {
        srand((unsigned int)time(NULL));
    }
    
    printf("Начало обработки...\n");
    start_timer(&metrics);

    /* Основной цикл обработки блоков с поддержкой многопроходности */
    size_t total_blocks_processed = 0;
    int pass;

    for (pass = 0; pass < cache_config.passes; pass++) {
        if (cache_config.passes > 1) {
            printf("Проход %d/%d...\n", pass + 1, cache_config.passes);
        }
        
        size_t blocks_processed = 0;
        size_t block_index;
        size_t current_pos;
        size_t max_pos;
        ssize_t result;
        
        /* Сбрасываем позицию в файле для каждого прохода (кроме первого, если это последовательный доступ) */
        if (pass > 0 || strcmp(type, "sequential") != 0) {
            if (strcmp(cache_config.type, "vtpc") == 0) {
                vtpc_lseek(vtpc_fd, start_pos, SEEK_SET);
            } else if (strcmp(cache_config.type, "direct") == 0) {
                lseek(std_fd, start_pos, SEEK_SET);
            } else {
                fseek(std_file, start_pos, SEEK_SET);
            }
        }
        
        for (block_index = 0; block_index < block_count; block_index++) {
            /* Определение позиции для текущего блока */
            if (strcmp(type, "sequential") == 0) {
                current_pos = start_pos + (block_index * block_size);
                if (!unlimited_range && current_pos + block_size > end_pos) {
                    break;
                }
            } else if (strcmp(type, "random") == 0) {
                if (range_size < block_size) {
                    break;
                }
                max_pos = end_pos - block_size;
                if (start_pos > max_pos) {
                    break;
                }
                current_pos = start_pos + (rand() % (max_pos - start_pos + 1));
                current_pos = start_pos + ((current_pos - start_pos) / block_size) * block_size;
            } else {
                printf("Неверный тип доступа. Используйте 'sequential' или 'random'\n");
                break;
            }
            
            /* Выполнение операции чтения/записи */
            if (strcmp(rw, "r") == 0) {
                result = 0;
                
                if (strcmp(cache_config.type, "vtpc") == 0) {
                    /* Чтение через наш кэш */
                    vtpc_lseek(vtpc_fd, current_pos, SEEK_SET);
                    result = vtpc_read(vtpc_fd, buffer, block_size);
                } else if (strcmp(cache_config.type, "direct") == 0) {
                    /* Прямое чтение */
                    lseek(std_fd, current_pos, SEEK_SET);
                    result = read(std_fd, buffer, block_size);
                } else {
                    /* Стандартное чтение */
                    fseek(std_file, current_pos, SEEK_SET);
                    result = fread(buffer, 1, block_size, std_file);
                }
                
                if (result > 0) {
                    blocks_processed++;
                    metrics.bytes_processed += result;
                    metrics.operations_count++;
                } else {
                    if (result == 0) {
                        printf("Достигнут конец файла\n");
                    } else {
                        perror("Ошибка чтения файла");
                    }
                    break;
                }
            } else {
                /* Операция записи */
                result = 0;
                
                /* Подготовка данных для записи */
                memset(buffer, 'A' + (block_index % 26), block_size);
                
                if (strcmp(cache_config.type, "vtpc") == 0) {
                    /* Запись через наш кэш */
                    vtpc_lseek(vtpc_fd, current_pos, SEEK_SET);
                    result = vtpc_write(vtpc_fd, buffer, block_size);
                } else if (strcmp(cache_config.type, "direct") == 0) {
                    /* Прямая запись */
                    if (current_pos + block_size > file_size) {
                        if (ftruncate(std_fd, current_pos + block_size) == -1) {
                            perror("Ошибка расширения размера файла");
                            break;
                        }
                        file_size = current_pos + block_size;
                    }
                    lseek(std_fd, current_pos, SEEK_SET);
                    result = write(std_fd, buffer, block_size);
                } else {
                    /* Стандартная запись */
                    if (current_pos + block_size > file_size) {
                        fseek(std_file, current_pos + block_size - 1, SEEK_SET);
                        fputc(0, std_file);
                        fflush(std_file);
                        file_size = current_pos + block_size;
                    }
                    fseek(std_file, current_pos, SEEK_SET);
                    result = fwrite(buffer, 1, block_size, std_file);
                }
                
                if (result == (ssize_t)block_size) {
                    blocks_processed++;
                    metrics.bytes_processed += result;
                    metrics.operations_count++;
                } else {
                    perror("Ошибка записи в файл");
                    break;
                }
            }
        }
        
        total_blocks_processed += blocks_processed;
        
        if (cache_config.passes > 1) {
            printf("  Проход %d завершен: обработано %zu блоков\n", pass + 1, blocks_processed);
        }
    }

    stop_timer(&metrics);

    printf("Успешно обработано блоков: %zu (за %d проходов)\n", total_blocks_processed, cache_config.passes);
    
    /* Вывод метрик производительности */
    print_metrics(&metrics, &cache_config);
    
    /* Вывод статистики кэша если используется vtpc */
    if (strcmp(cache_config.type, "vtpc") == 0) {
        vtpc_cache_stats();
    }

cleanup:
    /* Освобождение ресурсов */
    if (buffer) {
        free(buffer);
        buffer = NULL;
    }
    
    if (strcmp(cache_config.type, "vtpc") == 0) {
        if (vtpc_fd >= 0) {
            vtpc_close(vtpc_fd);
            vtpc_fd = -1;
        }
    } else if (strcmp(cache_config.type, "direct") == 0) {
        if (std_fd != -1) {
            close(std_fd);
            std_fd = -1;
        }
    } else {
        if (std_file != NULL) {
            fclose(std_file);
            std_file = NULL;
        }
    }
    
    return 0;
}