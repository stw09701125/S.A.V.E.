#include <iostream>
#include <map>
#include <unordered_map>
#include <set>
#include <vector>
#include <filesystem>
#include <fstream>
#include <shared_mutex>
#include <numeric>
#include <gperftools/profiler.h>
#include <csignal>
#include <exception>
#include <ctime>
#include <algorithm>
#include "thread_pool.hpp"

ThreadPool<void> thread_pool;
std::atomic<bool> quit(false);
std::unordered_map<std::string, std::pair<std::unique_ptr<std::mutex>, std::unique_ptr<std::ifstream>>> file_map;
std::unordered_map<std::string, std::string> last_total;
using TableType = std::map<std::string, std::unordered_map<std::string, std::vector<std::pair<std::streampos, std::streampos>>>>;

struct pair_hash
{
    template <class T1, class T2>
    std::size_t operator() (const std::pair<T1, T2> &pair) const
    {
        return std::hash<T1>()(pair.first) ^ std::hash<T2>()(pair.second);
    }
};
template <>
std::size_t pair_hash::operator() (const std::pair<std::streampos, std::streampos> &pair) const
{
    return std::hash<std::size_t>()(pair.first) ^ std::hash<std::size_t>()(pair.second);
}

struct EncodeString
{
  public:
    EncodeString(std::uint16_t r_mod, std::uint16_t r_len)
    : number    (r_len, 0)
    , mod       (r_mod)
    , overflow  (false)
    {}

    operator std::string()
    {
        std::string result;
        for (auto it = number.rbegin(); it != number.rend(); ++it)
            result.push_back('A' + *it);
        return result;
    }

    EncodeString& operator++()
    {
        ++number[0];
        for (std::size_t i = 0; i < number.size(); ++i)
        {
            if (number[i] == mod)
            {
                number[i] = 0;
                if (i + 1 != number.size())
                    ++number[i + 1];
                else
                    overflow = true;
            }
            else
                break;   
        }
        return *this;
    }

    operator bool()
    {
        return !overflow;
    }

    EncodeString& operator=(const std::string& rhs)
    {
        if (rhs.size() != number.size())
            std::cout << "Size is not correct\n";
        int r_mod;
        for (int i = 0; i < number.size(); ++i)
        {
            r_mod = rhs[number.size() - i - 1] - 'A';
            if (r_mod >= mod)
                std::cout << "String is not ligal\n";
            else
                number[i] = static_cast<std::uint8_t>(r_mod);
        }
        return *this;
    }

    bool operator==(const std::string& rhs)
    {
        std::size_t len = number.size();
        for (int i = 0u; i < len; ++i)
        {
            if (number[i] + 'A' != rhs[len - i - 1])
                return false;
        }
        return true;
    }

  private:
    bool overflow;
    std::uint16_t mod;
    std::vector<std::uint8_t> number;  
};

class EncodeTable
{
  public:
    friend void merge_table(std::set<std::string>& total_table, std::ifstream& infile, std::mutex& table_mutex, const std::string current_filename);

    // EncodeTable(const std::filesystem::path& database_path)
    // {
    //     for (auto& file : std::filesystem::directory_iterator(database_path))
    //         file_map.emplace_hint(file_map.cend(), file.path().filename(), std::make_pair(new std::mutex{}, new std::ifstream(file.path())));
    // }

    void clear()
    {
        std::unique_lock<std::shared_mutex> lock(mutex);
        table.clear();
        cache.clear();
        cache_queue.clear();
    }

    double calc_fp()
    {
        std::shared_lock<std::shared_mutex> lock(mutex);

        double fp = 0;
        std::uint32_t total = 0;
        for (const auto& encode_item : table)
        {
            if (encode_item.second.size() == 1 && (encode_item.second.begin()->second).size() == 1)
                ++total;
            else
            {
                for (const auto& file_item : encode_item.second)
                {
                    total += file_item.second.size();
                    fp += file_item.second.size();
                }
            }
        }
        return fp / total;
    }

    bool exists(const std::string& encode_string, const std::string& origin_string, const std::string except_filename = "")
    {
        std::shared_lock<std::shared_mutex> lock(mutex);

        std::size_t len;
        bool found = false;
        std::string buf_string;

        if (table.find(encode_string) == table.end())
            return false;

        // iterate on table
        for (auto& [filename, positions] : table[encode_string])
        {
            if (!except_filename.empty() && filename == except_filename)
                continue;
            std::unique_lock<std::mutex> file_lock(*(file_map[filename].first));
            auto& infile = *(file_map[filename].second);
            for (auto& pos : positions)
            {
                // check whether size is equal
                len = pos.second - pos.first;
                if (origin_string.size() != len)
                    continue;
                // check cache
                // else if (cache.find(pos) != cache.end())
                //     buf_string = cache[pos];
                else // then readout file
                {
                    infile.seekg(pos.first);
                    buf_string.resize(pos.second - pos.first);
                    infile.read(&buf_string[0], buf_string.size());
                }
                // std::cerr << "filename " << filename << std::endl;
                // std::cerr << "pos.first " << pos.first << std::endl;
                // std::cerr << "pos.second " << pos.second << std::endl;
                // std::cerr << "buf_string " << buf_string << std::endl;
                // std::cerr << "origin_string " << origin_string << std::endl;
                if (buf_string == origin_string)
                {
                    found = true;
                    break;
                }
            }
            if (found)
                break;
        }
        return found;
    }

    void update(const std::string& encode_string, const std::string& filename, std::pair<std::streampos, std::streampos> range, const std::string& origin_string)
    {
        std::unique_lock<std::shared_mutex> lock(mutex);
        table[encode_string][filename].emplace_back(range);
        // update cache
        // if (cache_queue.size() == CACHE_SIZE)
        // {
        //     cache.erase(cache_queue.front());
        //     cache_queue.pop_front();
        // }
        // auto it = cache.emplace_hint(cache.cend(), range, origin_string);
        // cache_queue.push_back(range);
    }

    void save(const std::filesystem::path& table_path, std::uint16_t mod, std::uint16_t len)
    {
        std::size_t buf_size;
        std::string buf_string;
        std::ofstream outfile(table_path, std::ios::binary);
        // iterate on table
        for (auto& [encode_string, filenames] : table)
        {
            // encode string 
            buf_size = encode_string.size();
            outfile.write(reinterpret_cast<char*>(&buf_size), sizeof(std::size_t));
            outfile.write(&encode_string[0], encode_string.size());
            // filenames size
            buf_size = filenames.size();
            outfile.write(reinterpret_cast<char*>(&buf_size), sizeof(std::size_t));
            for (auto& [filename, positions] : filenames)
            {
                std::unique_lock<std::mutex> file_lock(*(file_map[filename].first));
                auto& infile = *(file_map[filename].second);
                // filename
                buf_size = filename.size();
                outfile.write(reinterpret_cast<char*>(&buf_size), sizeof(std::size_t));
                outfile.write(&filename[0], buf_size);
                // positions size
                buf_size = positions.size();
                outfile.write(reinterpret_cast<char*>(&buf_size), sizeof(std::size_t));
                for (auto& pos : positions)
                {
                    infile.seekg(pos.first);
                    buf_string.resize(pos.second - pos.first);
                    buf_size = buf_string.size();
                    infile.read(&buf_string[0], buf_size);
                    // origin string TODO: write position not string
                    outfile.write(reinterpret_cast<char*>(&buf_size), sizeof(std::size_t));
                    outfile.write(&buf_string[0], buf_size);
                }
            }
        }
    }

  private:
    TableType table;
    mutable std::shared_mutex mutex;
    static const std::uint32_t CACHE_SIZE = 1000000u;
    std::unordered_map<std::pair<std::streampos, std::streampos>, std::string, pair_hash> cache;
    // std::deque<std::unordered_map<std::pair<std::streampos, std::streampos>, std::string>::const_iterator> cache_queue;
    std::deque<decltype(cache)::key_type> cache_queue;
};

void merge_table(std::unordered_map<std::string, std::uint16_t>& total_table, std::ifstream& infile, std::mutex& table_mutex, const std::string current_filename)
{
    std::string buf_string, encode_string, filename;
    std::size_t buf_size, filenames_len, positions_len;
    auto insert_it = total_table.begin();
    // std::string total;
    try
    {
        // filenames size
        infile.read(reinterpret_cast<char*>(&filenames_len), sizeof(std::size_t));
        // total += std::to_string(filenames_len) + " ";
        for (auto i = 0u; i < filenames_len; ++i)
        {
            // filename
            infile.read(reinterpret_cast<char*>(&buf_size), sizeof(std::size_t));
            filename.resize(buf_size);
            // total += std::to_string(buf_size) + " ";
            infile.read(&filename[0], buf_size);
            // total += filename + " ";
            if (filename != current_filename)
            {
                std::cerr << "Error: filename != current_filename ( " << filename << " != " << current_filename << "\n";
                throw std::invalid_argument("filename != current_filename");
            }
            // positions size
            infile.read(reinterpret_cast<char*>(&positions_len), sizeof(std::size_t));
            // total += std::to_string(positions_len) + " ";
            for (auto j = 0u; j < positions_len; ++j)
            {
                // origin string
                infile.read(reinterpret_cast<char*>(&buf_size), sizeof(std::size_t));
                buf_string.resize(buf_size); // TODO: bad_alloc
                // total += std::to_string(buf_size) + " ";
                infile.read(&buf_string[0], buf_size);
                // total += buf_string + " ";
                std::unique_lock<std::mutex> lock(table_mutex);
                if ((insert_it = total_table.find(buf_string)) == total_table.end())
                    total_table.emplace_hint(total_table.end(), buf_string, 1);
                else
                    ++(insert_it->second);
            }
        }
        // last_total[filename] = total;
    }
    catch(const std::runtime_error& re)
    {
        // speciffic handling for runtime_error
        std::cerr << "Runtime error: " << re.what() << std::endl;
        std::cerr << "tellg(): " << infile.tellg() << std::endl;
        // std::cerr << "Total: " << total << std::endl;
        std::cerr << "Last total: " << last_total[filename] << std::endl;
        exit(0);
    }
    catch(const std::exception& ex)
    {
        // speciffic handling for all exceptions extending std::exception, except
        // std::runtime_error which is handled explicitly
        std::cerr << "Error occurred: " << ex.what() << std::endl;
        std::cerr << "tellg(): " << infile.tellg() << std::endl;
        // std::cerr << "Total: " << total << std::endl;
        std::cerr << "Last total: " << last_total[filename] << std::endl;
        exit(0);
    }
    catch(...)
    {
        // catch any other errors (that we have no information about)
        std::cerr << "Unknown failure occurred. Possible memory corruption" << std::endl;
        std::cerr << "tellg(): " << infile.tellg() << std::endl;
        // std::cerr << "Total: " << total << std::endl;
        std::cerr << "Last total: " << last_total[filename] << std::endl;
        exit(0);
    }
}

void find_string(std::map<std::string, std::size_t>& reload_pos, std::filesystem::path path, std::string encode_string)
{
    std::size_t buf_size, positions_len;
    std::string buf_string;
    std::ifstream infile(path);
    while (true)
    {
        infile.read(reinterpret_cast<char*>(&buf_size), sizeof(std::size_t));
        buf_string.resize(buf_size);
        infile.read(&buf_string[0], buf_size);
        if (buf_string >= std::string(encode_string))
            break;
        // filenames size
        infile.read(reinterpret_cast<char*>(&buf_size), sizeof(std::size_t));
        // filename
        infile.seekg(buf_size, std::ios::cur);
        // positions size
        infile.read(reinterpret_cast<char*>(&positions_len), sizeof(std::size_t));
        for (auto j = 0u; j < positions_len; ++j)
        {
            // origin string
            infile.read(reinterpret_cast<char*>(&buf_size), sizeof(std::size_t));
            infile.seekg(buf_size, std::ios::cur);
        }
    }
    reload_pos.emplace_hint(reload_pos.cend(), path.filename(), buf_size);
    std::cerr << "Finish: " << path.filename() << std::endl;
}

bool is_number(const std::string& filename)
{
    bool result = true;
    for (auto& c : filename)
    {
        if (c < '0' || c > '9')
        {
            result = false;
            break;
        }
    }
    return result;
}

void encode_file(const std::filesystem::path in_path, const std::filesystem::path out_path, std::uint8_t mod)
{
    std::ifstream infile(in_path);
    std::ofstream outfile(out_path);
    std::string word, encode_doc;
    std::uint16_t ord;
    while (infile >> word)
    {
        // read a page
        while (true)
        {
            if (word != "</doc>")
            {
                ord = 0;
                for (auto& c : word)
                    ord += c;
                encode_doc.append(1, 'A' + ord % mod);
            }
            else
                break;
            infile >> word;
        }
        outfile << encode_doc << "\n";
        encode_doc.clear();
    }
}

void build_table(const std::filesystem::path doc_path, const std::filesystem::path encode_path, std::uint16_t mod, std::uint16_t len, const std::filesystem::path output_path)
{
    std::ifstream word_file(doc_path);
    std::ifstream mod_file(encode_path);
    std::string buf_string, encode_string, origin_string_all;
    std::deque<std::string> origin_string;
    std::deque<std::streampos> word_pos;
    std::string filename = doc_path.filename();
    int doc_num = 0;
    EncodeTable table;
    // std::string total;

    // start build
    while (getline(mod_file, encode_string) && !quit.load())
    {
        ++doc_num;
        // total.clear();
        // jump through too small page
        if (encode_string.size() < len)
        {
            for (auto i = 0u; i < encode_string.size() + 1; ++i)
                word_file >> buf_string;
            continue;
        }
        // warmup
        origin_string.clear();
        word_pos.clear();
        word_pos.emplace_back(word_file.tellg());
        for (auto i = 0u; i < len; ++i)
        {
            word_file >> buf_string;
            origin_string.emplace_back(buf_string + ' ');
            // total += origin_string.back();
            word_pos.emplace_back(word_file.tellg() + 1l);
        }
        // sliding window
        for (auto i = 0u; i < encode_string.size() - len; ++i)
        {
            // update dict
            buf_string = encode_string.substr(i, len);
            origin_string_all = std::accumulate(origin_string.begin(), origin_string.end(), std::string{});
            if (!table.exists(buf_string, origin_string_all))
                table.update(buf_string, filename, std::make_pair(word_pos.front(), word_pos.back()), origin_string_all);
            // std::cerr << table.exists(buf_string, std::accumulate(origin_string.begin(), origin_string.end(), std::string{})) << std::endl;
            // move forward
            word_pos.pop_front();
            origin_string.pop_front();
            word_file >> buf_string;
            origin_string.emplace_back(buf_string + ' ');
            // total += origin_string.back();
            word_pos.emplace_back(word_file.tellg() + 1l);
        }
        word_file >> buf_string; // jump through </doc>
        if (buf_string != "</doc>")
        {
            std::cout << doc_num << " " << doc_path << " " << encode_string << std::endl;
            // std::cout << total << buf_string << std::endl;
            exit(0);
        }
    }
    table.save(output_path, mod, len);
}

void got_signal(int signal)
{
    std::cerr << "Signal raised: " << signal << "\n";
    quit.store(true);
}

int main(int argc, char** argv)
{
    std::signal(SIGINT, got_signal);
    // ProfilerStart("test.prof");
    std::filesystem::path database_path = argv[1];
    std::filesystem::path table_path = "/mnt/prx_work1/WikiDB";

    std::vector<std::uint16_t> mod_space = {(std::uint16_t)std::stoi(argv[2])};
    std::vector<std::uint16_t> compare_len = {(std::uint16_t)std::stoi(argv[3])};

    if (!std::filesystem::exists(database_path / "encoded"))
        std::filesystem::create_directory(database_path / "encoded");
    if (!std::filesystem::exists(table_path / "table"))
        std::filesystem::create_directory(table_path / "table");

    unsigned int remain;
    std::uint64_t fp, total;
    std::mutex table_mutex;
    std::unordered_map<std::string, std::uint16_t> total_table;
    std::string output_table_path;
    std::filesystem::path mod_path;
    time_t timer = time(nullptr);
    for (auto& mod : mod_space)
    {
        mod_path = database_path / "encoded" / std::to_string(mod);
        if (!std::filesystem::exists(mod_path))
            std::filesystem::create_directory(mod_path);
        for (auto& file : std::filesystem::directory_iterator(database_path))
        {
            if (is_number(file.path().filename().string()) && !std::filesystem::exists(mod_path / file.path().filename()))
                thread_pool.submit(std::bind(encode_file, file.path(), mod_path / file.path().filename(), mod));
        }
        while (!thread_pool.is_idle() && !quit.load())
        {
            std::this_thread::sleep_for(std::chrono::seconds(10));
            remain = thread_pool.get_task_num();
            std::cerr << "[build_mod_db (mod = " << mod << ")] Task remain: " << remain << "\n";
        }
        if (quit.load())
        {
            std::cerr << "Wait threads to complete remaining tasks ...\n";
            thread_pool.terminate_all_thread();
            break;
        }

        for (auto& len : compare_len)
        {
            fp = 0;
            total = 0;
            output_table_path = table_path / "table" / ("mod_" + std::to_string(mod) + "_len_" + std::to_string(len));
            if (!std::filesystem::exists(output_table_path))
                std::filesystem::create_directory(output_table_path);
            // build table
            for (auto& file : std::filesystem::directory_iterator(database_path))
                file_map.emplace_hint(file_map.cend(), file.path().filename(), std::make_pair(new std::mutex{}, new std::ifstream(file.path())));
            // int i = 0;
            for (auto& file : std::filesystem::directory_iterator(database_path))
            {
                if (is_number(file.path().filename().string()) && !std::filesystem::exists(output_table_path / file.path().filename()))
                    thread_pool.submit(std::bind(build_table, file.path(), mod_path / file.path().filename(), mod, len, output_table_path / file.path().filename()));
                // if (++i == 3)
                //     break;
            }
            std::this_thread::sleep_for(std::chrono::seconds(3));
            while (!thread_pool.is_idle() && !quit.load())
            {
                std::this_thread::sleep_for(std::chrono::seconds(10));
                remain = thread_pool.get_task_num();
                std::cerr << "[build_table (mod = " << mod << ", len = " << len << ")] Task remain: " << remain << "\n";
            }
            if (quit.load())
            {
                std::cerr << "Wait threads to complete remaining tasks ...\n";
                // ProfilerStop();
                thread_pool.terminate_all_thread();
                break;
            }
            // merge table
            EncodeString encode_string(mod, len);
            // encode_string = "JILNJLIHHDIL";
            std::unordered_map<std::string, std::string> current_key;
            std::set<std::string> submit_list;
            std::size_t buf_size;
            std::string buf_string;
            file_map.clear();

            std::map<std::string, std::size_t> reload_pos;
            if (std::filesystem::exists("result_mod" + std::to_string(mod) + "_len" + std::to_string(len) + "_reload.txt"))
            {
                std::ifstream reload_file("result_mod" + std::to_string(mod) + "_len" + std::to_string(len) + "_reload.txt");
                for (int i = 0; i < 5; ++i) std::getline(reload_file, buf_string);
                while (reload_file >> buf_string)
                {
                    reload_file >> buf_size;
                    reload_pos.emplace_hint(reload_pos.cend(), buf_string, buf_size);
                }
            }
            // else if (encode_string != "AAAAAAAAAAAA")
            // {
            //     for (auto& file : std::filesystem::directory_iterator(output_table_path))
            //         thread_pool.submit(std::bind(find_string, std::ref(reload_pos), file.path(), encode_string));
            //     while (!thread_pool.is_idle() && !quit.load())
            //     {
            //         std::this_thread::sleep_for(std::chrono::seconds(10));
            //         remain = thread_pool.get_task_num();
            //         std::cerr << "[find_string (mod = " << mod << ", len = " << len << ")] Task remain: " << remain << "\n";
            //     }
            //     if (quit.load())
            //     {
            //         std::cerr << "Wait threads to complete remaining tasks ...\n";
            //         // ProfilerStop();
            //         thread_pool.terminate_all_thread();
            //         exit(0);
            //     }
            // }

            for (auto& file : std::filesystem::directory_iterator(output_table_path))
            {
                if (!is_number(file.path().filename().string()))
                    continue;
                file_map.emplace_hint(file_map.cend(), file.path().filename(), std::make_pair(new std::mutex{}, new std::ifstream(file.path())));
                auto& infile = file_map[file.path().filename()].second;
                if (!reload_pos.empty())
                    infile->seekg(reload_pos[file.path().filename()] - len - sizeof(std::size_t));
                infile->read(reinterpret_cast<char*>(&buf_size), sizeof(std::size_t));
                buf_string.resize(buf_size);
                infile->read(&buf_string[0], buf_size);
                current_key.emplace_hint(current_key.cend(), file.path().filename(), buf_string);
            }
            if (reload_pos.empty())
            {
                for (auto& [filename, current_string] : current_key)
                    reload_pos.emplace_hint(reload_pos.cend(), filename, len + sizeof(std::size_t));
            }

            std::ofstream count_recorder("counter_mod" + std::to_string(mod) + "_len" + std::to_string(len) + ".txt");
            do
            {
                // find next encode string
                encode_string = std::min_element(current_key.begin(), current_key.end(), [](const auto& lhs, const auto& rhs) {return lhs.second < rhs.second;})->second;
                // find those file has encode_string then submit
                for (auto& [filename, current_string] : current_key)
                {
                    if (encode_string == current_string)
                        submit_list.insert(filename);
                }
                for (auto& filename : submit_list)
                    thread_pool.submit(std::bind(merge_table, std::ref(total_table), std::ref(*file_map[filename].second), std::ref(table_mutex), filename));
                // std::this_thread::sleep_for(std::chrono::seconds(3));
                while (!thread_pool.is_idle() && !quit.load())
                {
                    // std::this_thread::sleep_for(std::chrono::seconds(10));
                    // remain = thread_pool.get_task_num();
                    // std::cerr << "[merge_table (mod = " << mod << ", len = " << len << ", encode_string = " << std::string(encode_string) << ")] Task remain: " << remain << "\n";
                }
                if (quit.load())
                {
                    std::cerr << "Wait threads to complete remaining tasks ...\n";
                    // ProfilerStop();
                    thread_pool.terminate_all_thread();
                    break;
                }
                // move to next encode_string
                for (auto& filename : submit_list)
                {
                    auto& infile = file_map[filename].second;
                    infile->read(reinterpret_cast<char*>(&buf_size), sizeof(std::size_t));
                    if (!infile)
                    {
                        current_key.erase(filename);
                        std::cerr << filename << " finished.\n";
                        continue;
                    }
                    buf_string.resize(buf_size);
                    infile->read(&buf_string[0], buf_size);
                    if (!infile)
                    {
                        current_key.erase(filename);
                        std::cerr << filename << " finished.\n";
                        continue;
                    }
                    current_key[filename] = buf_string;
                    reload_pos[filename] = infile->tellg();
                }
                // calculate false positive
                if (total_table.size() == 1)
                {
                    count_recorder << std::string(encode_string) << " " << total_table.begin()->second << "\n";
                    ++total;
                }
                else
                {
                    count_recorder << std::string(encode_string);
                    for (auto it = total_table.begin(); it != total_table.end(); ++it)
                        count_recorder << " " << it->second;
                    count_recorder << "\n";
                    fp += total_table.size();
                    total += total_table.size();
                }
                if (difftime(time(nullptr), timer) > 30 && total != 0)
                {
                    std::cerr << "[False positive rate (mod = " << mod << ", len = " << len << ", encode_string = " << std::string(encode_string) << ")] : " << fp << " / " << total << "\n";
                    std::ofstream outfile("result_mod" + std::to_string(mod) + "_len" + std::to_string(len) + ".txt");
                    outfile << "total: " << total << std::endl;
                    outfile << "fp: " << fp << std::endl;
                    outfile << "False positive rate: " << fp * 1.0 / total << std::endl;
                    outfile << "Last string: " << std::string(encode_string) << std::endl;
                    outfile << "tellg() list:\n";
                    for (const auto& [filename, pos] : reload_pos)
                        outfile << filename << " " << pos << std::endl;
                    timer = time(nullptr);
                }
                while (!thread_pool.is_idle() && !quit.load()) {} // prevent double free
                total_table.clear();
                submit_list.clear();
            } while (!current_key.empty());
            std::ofstream outfile("result_mod" + std::to_string(mod) + "_len" + std::to_string(len) + ".txt");
            outfile << "total: " << total << std::endl;
            outfile << "fp: " << fp << std::endl;
            outfile << "False positive rate: " << fp * 1.0 / total << std::endl;
            outfile << "Last string: " << std::string(encode_string) << std::endl;
            outfile << "tellg() list:\n";
            for (const auto& [filename, pos] : reload_pos)
                outfile << filename << " " << pos << std::endl;
            file_map.clear();
            if (quit.load())
                break;
            // std::filesystem::remove_all(output_table_path);
        }
        if (quit.load())
            break;
    }
    // ProfilerStop();
}